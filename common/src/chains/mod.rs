use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use alloy_primitives::{BlockHash, BlockNumber};
use eventuals::{Eventual, EventualWriter};
use indexer_selection::UnresolvedBlock;
use prelude::epoch_cache::EpochCache;
use thegraph::types::BlockPointer;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use tracing::Instrument;

use crate::{block_constraints::BlockConstraint, metrics::*};

pub mod block_rate;
pub mod ethereum;
pub mod test;

#[derive(Debug, Clone)]
pub struct BlockHead {
    pub block: BlockPointer,
    pub uncles: Vec<BlockHash>,
}

pub trait Client {
    type Config;

    fn chain_name(config: &Self::Config) -> &str;

    fn poll_interval() -> Duration;

    fn create(
        config: Self::Config,
        notify: mpsc::UnboundedSender<ClientMsg>,
    ) -> mpsc::UnboundedSender<UnresolvedBlock>;
}

#[derive(Debug)]
pub enum ClientMsg {
    Head(BlockHead),
    Block(BlockPointer),
    Err(UnresolvedBlock),
}

#[derive(Default)]
pub struct BlockRequirements {
    pub constraints: BTreeSet<BlockConstraint>,
    pub resolved: BTreeSet<BlockPointer>,
}

#[derive(Clone)]
pub struct BlockCache {
    pub chain_head: Eventual<BlockPointer>,
    pub blocks_per_minute: Eventual<u64>,
    request_tx: mpsc::UnboundedSender<Request>,
}

struct Request {
    unresolved: UnresolvedBlock,
    tx: oneshot::Sender<Result<BlockPointer, UnresolvedBlock>>,
}

impl BlockCache {
    pub fn new<C: Client>(config: C::Config) -> Self {
        let (chain_head_tx, chain_head_rx) = Eventual::new();
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (blocks_per_minute_tx, blocks_per_minute_rx) = Eventual::new();
        let actor = Actor {
            chain: C::chain_name(&config).to_string(),
            client_tx: C::create(config, notify_tx),
            notify_rx,
            request_rx,
            chain_head_tx,
            blocks_per_minute_tx,
            block_rate_estimator: block_rate::Estimator::new(C::poll_interval()),
            number_to_hash: EpochCache::new(),
            hash_to_number: EpochCache::new(),
            pending: HashMap::new(),
        };
        actor.spawn();
        Self {
            chain_head: chain_head_rx,
            blocks_per_minute: blocks_per_minute_rx,
            request_tx,
        }
    }

    pub async fn fetch_block(
        &self,
        unresolved: UnresolvedBlock,
    ) -> Result<BlockPointer, UnresolvedBlock> {
        let (tx, rx) = oneshot::channel();
        let request = Request { unresolved, tx };
        self.request_tx.send(request).ok().unwrap();
        rx.await.unwrap()
    }
}

struct Actor {
    chain: String,
    request_rx: mpsc::UnboundedReceiver<Request>,
    client_tx: mpsc::UnboundedSender<UnresolvedBlock>,
    notify_rx: mpsc::UnboundedReceiver<ClientMsg>,
    chain_head_tx: EventualWriter<BlockPointer>,
    blocks_per_minute_tx: EventualWriter<u64>,
    block_rate_estimator: block_rate::Estimator,
    number_to_hash: EpochCache<BlockNumber, BlockHash, 2>,
    hash_to_number: EpochCache<BlockHash, BlockNumber, 2>,
    pending: HashMap<UnresolvedBlock, Vec<oneshot::Sender<Result<BlockPointer, UnresolvedBlock>>>>,
}

impl Actor {
    fn spawn(mut self) {
        let _trace = tracing::info_span!("block_cache", chain = %self.chain).entered();
        tokio::spawn(
            async move {
                let mut cache_timer = interval(Duration::from_secs(32));
                loop {
                    tokio::select! {
                        _ = cache_timer.tick() => {
                            self.number_to_hash.increment_epoch();
                            self.hash_to_number.increment_epoch();
                        },
                        Some(request) = self.request_rx.recv() => {
                            self.handle_request(request).await;
                        },
                        Some(msg) = self.notify_rx.recv() => match msg {
                            ClientMsg::Head(head) => self.handle_chain_head(head).await,
                            ClientMsg::Block(block) => self.handle_block(block).await,
                            ClientMsg::Err(unresolved) => self.handle_err(unresolved).await,
                        },
                        else => break,
                    };
                }
                tracing::error!("exit");
            }
            .in_current_span(),
        );
    }

    async fn handle_request(&mut self, request: Request) {
        let cached = match request.unresolved {
            UnresolvedBlock::WithHash(hash) => self
                .hash_to_number
                .get(&hash)
                .map(|&number| BlockPointer { hash, number })
                .ok_or(UnresolvedBlock::WithHash(hash)),
            UnresolvedBlock::WithNumber(number) => self
                .number_to_hash
                .get(&number)
                .map(|&hash| BlockPointer { hash, number })
                .ok_or(UnresolvedBlock::WithNumber(number)),
        };
        match cached {
            Ok(block) => {
                with_metric(&METRICS.block_cache_hit, &[&self.chain], |c| c.inc());
                let _ = request.tx.send(Ok(block));
            }
            Err(unresolved) => {
                with_metric(&METRICS.block_cache_miss, &[&self.chain], |c| c.inc());
                let _ = self.client_tx.send(unresolved.clone());
                self.pending.entry(unresolved).or_default().push(request.tx);
            }
        }
    }

    async fn handle_chain_head(&mut self, head: BlockHead) {
        for uncle in &head.uncles {
            let removed = self.hash_to_number.remove(uncle);
            if let Some(removed) = removed {
                self.number_to_hash.remove(&removed);
            }
        }

        let blocks_per_minute = self.block_rate_estimator.update(head.block.number);
        self.blocks_per_minute_tx.write(blocks_per_minute);

        tracing::info!(chain_head = ?head, blocks_per_minute);

        // Remove prior blocks from the past minute. This avoids retaining uncled blocks that are
        // frequently used.
        let cutoff = head.block.number - blocks_per_minute;
        self.number_to_hash.retain(|n, _| *n < cutoff);
        self.hash_to_number.retain(|_, n| *n < cutoff);

        self.handle_block(head.block.clone()).await;

        with_metric(&METRICS.chain_head, &[&self.chain], |g| {
            g.set(head.block.number as i64)
        });
        with_metric(&METRICS.blocks_per_minute, &[&self.chain], |g| {
            g.set(blocks_per_minute as i64)
        });

        self.chain_head_tx.write(head.block);
    }

    async fn handle_block(&mut self, block: BlockPointer) {
        self.hash_to_number.insert(block.hash, block.number);
        self.number_to_hash.insert(block.number, block.hash);

        for key in [
            UnresolvedBlock::WithHash(block.hash),
            UnresolvedBlock::WithNumber(block.number),
        ] {
            let waiters = match self.pending.remove(&key) {
                Some(waiters) => waiters,
                None => continue,
            };
            for tx in waiters {
                let _ = tx.send(Ok(block.clone()));
            }
        }
    }

    async fn handle_err(&mut self, unresolved: UnresolvedBlock) {
        let waiters = match self.pending.remove(&unresolved) {
            Some(waiters) => waiters,
            None => return,
        };
        for tx in waiters {
            let _ = tx.send(Err(unresolved.clone()));
        }
    }
}
