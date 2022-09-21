pub mod ethereum;
pub mod test;

use crate::{block_constraints::*, metrics::*};
use actix_web::Result;
use indexer_selection::UnresolvedBlock;
use prelude::{epoch_cache::EpochCache, tokio::time::interval, *};
use std::collections::{BTreeSet, HashMap};

pub trait Provider {
    fn network(&self) -> &str;
}

pub trait Client {
    type Provider: Provider;
    fn new(
        provider: Self::Provider,
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
    request_tx: mpsc::UnboundedSender<Request>,
}

struct Request {
    unresolved: UnresolvedBlock,
    tx: oneshot::Sender<Result<BlockPointer, UnresolvedBlock>>,
}

impl BlockCache {
    pub fn new<C: Client>(provider: C::Provider) -> Self {
        let (chain_head_broadcast_tx, chain_head_broadcast_rx) = Eventual::new();
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let actor = Actor {
            network: provider.network().to_string(),
            client_tx: C::new(provider, notify_tx),
            notify_rx,
            request_rx,
            chain_head_broadcast_tx,
            number_to_hash: EpochCache::new(),
            hash_to_number: EpochCache::new(),
            pending: HashMap::new(),
        };
        actor.spawn();
        Self {
            chain_head: chain_head_broadcast_rx,
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

    pub async fn latest(&mut self, skip: u64) -> Result<BlockPointer, UnresolvedBlock> {
        let latest = self
            .chain_head
            .value_immediate()
            .ok_or(UnresolvedBlock::WithNumber(0))?;
        let number = latest.number.saturating_sub(skip);
        if number == latest.number {
            return Ok(latest);
        }
        self.fetch_block(UnresolvedBlock::WithNumber(number)).await
    }
}

struct Actor {
    network: String,
    request_rx: mpsc::UnboundedReceiver<Request>,
    client_tx: mpsc::UnboundedSender<UnresolvedBlock>,
    notify_rx: mpsc::UnboundedReceiver<ClientMsg>,
    chain_head_broadcast_tx: EventualWriter<BlockPointer>,
    number_to_hash: EpochCache<u64, Bytes32, 2>,
    hash_to_number: EpochCache<Bytes32, u64, 2>,
    pending: HashMap<UnresolvedBlock, Vec<oneshot::Sender<Result<BlockPointer, UnresolvedBlock>>>>,
}

impl Actor {
    fn spawn(mut self) {
        let _trace = tracing::info_span!("Block Cache Actor", network = %self.network).entered();
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
                tracing::error!("block cache exit");
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
                with_metric(&METRICS.block_cache_hit, &[&self.network], |c| c.inc());
                let _ = request.tx.send(Ok(block));
            }
            Err(unresolved) => {
                with_metric(&METRICS.block_cache_miss, &[&self.network], |c| c.inc());
                let _ = self.client_tx.send(unresolved.clone());
                self.pending.entry(unresolved).or_default().push(request.tx);
            }
        }
    }

    async fn handle_chain_head(&mut self, head: BlockHead) {
        tracing::info!(chain_head = ?head);

        for uncle in &head.uncles {
            let removed = self.hash_to_number.remove(uncle);
            if let Some(removed) = removed {
                self.number_to_hash.remove(&removed);
            }
        }

        self.handle_block(head.block.clone()).await;

        with_metric(&METRICS.chain_head, &[&self.network], |g| {
            g.set(head.block.number as i64)
        });
        self.chain_head_broadcast_tx.write(head.block);
    }

    async fn handle_block(&mut self, block: BlockPointer) {
        self.hash_to_number.insert(block.hash.clone(), block.number);
        self.number_to_hash.insert(block.number, block.hash.clone());

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
