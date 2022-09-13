pub mod ethereum;
pub mod test;

use crate::{block_constraints::*, metrics::*};
use actix_web::Result;
use indexer_selection::UnresolvedBlock;
use prelude::{epoch_cache::EpochCache, tokio::time::interval, *};
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    mem,
};

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

pub struct BlockRequirements {
    pub constraints: BTreeSet<BlockConstraint>,
    pub resolved: BTreeSet<BlockPointer>,
}

impl BlockRequirements {
    fn resolved(&self) -> bool {
        self.constraints.iter().all(|c| match c {
            BlockConstraint::Unconstrained => true,
            BlockConstraint::Hash(h) => self.resolved.iter().any(|b| &b.hash == h),
            BlockConstraint::Number(n) | BlockConstraint::NumberGTE(n) => {
                self.resolved.iter().any(|b| &b.number == n)
            }
        })
    }

    pub fn has_latest(&self) -> bool {
        self.constraints.iter().any(|c| match c {
            BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => true,
            BlockConstraint::Hash(_) | BlockConstraint::Number(_) => false,
        })
    }
}

#[derive(Clone)]
pub struct BlockCache {
    pub chain_head: Eventual<BlockPointer>,
    request_tx: mpsc::UnboundedSender<Request>,
}

enum Request {
    Block {
        number: u64,
        tx: oneshot::Sender<Result<BlockPointer, UnresolvedBlock>>,
    },
    Requirements {
        constraints: BTreeSet<BlockConstraint>,
        tx: oneshot::Sender<Result<BlockRequirements, UnresolvedBlock>>,
    },
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
            pending_block: BTreeMap::new(),
            pending_requirements: Vec::new(),
        };
        actor.spawn();
        Self {
            chain_head: chain_head_broadcast_rx,
            request_tx,
        }
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

        let (tx, rx) = oneshot::channel();
        let request = Request::Block { number, tx };
        self.request_tx.send(request).ok().unwrap();
        Ok(rx.await.unwrap().unwrap_or(latest))
    }

    pub async fn block_requirements(
        &mut self,
        constraints: BTreeSet<BlockConstraint>,
    ) -> Result<BlockRequirements, UnresolvedBlock> {
        let (tx, rx) = oneshot::channel();
        let request = Request::Requirements { constraints, tx };
        self.request_tx.send(request).ok().unwrap();
        rx.await.unwrap()
    }
}

struct Actor {
    network: String,
    client_tx: mpsc::UnboundedSender<UnresolvedBlock>,
    notify_rx: mpsc::UnboundedReceiver<ClientMsg>,
    request_rx: mpsc::UnboundedReceiver<Request>,
    chain_head_broadcast_tx: EventualWriter<BlockPointer>,
    number_to_hash: EpochCache<u64, Bytes32, 2>,
    hash_to_number: EpochCache<Bytes32, u64, 2>,
    pending_block: BTreeMap<u64, Vec<oneshot::Sender<Result<BlockPointer, UnresolvedBlock>>>>,
    pending_requirements: Vec<PendingRequirementsRequest>,
}

struct PendingRequirementsRequest {
    unresolved: BTreeSet<UnresolvedBlock>,
    requirements: BlockRequirements,
    tx: oneshot::Sender<Result<BlockRequirements, UnresolvedBlock>>,
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
                        }
                        Some(msg) = self.notify_rx.recv() => match msg {
                            ClientMsg::Head(head) => self.handle_chain_head(head).await,
                            ClientMsg::Block(block) => self.handle_block(block).await,
                            ClientMsg::Err(unresolved) => self.handle_err(unresolved).await,
                        },
                        Some(request) = self.request_rx.recv() => match request {
                            Request::Block { number, tx } => {
                                self.handle_block_request(number, tx).await;
                            }
                            Request::Requirements{ constraints, tx } => {
                                self.handle_block_requirements(constraints, tx).await;
                            }
                        },
                        else => break,
                    };
                }
                tracing::error!("block cache exit");
            }
            .in_current_span(),
        );
    }

    async fn handle_chain_head(&mut self, head: BlockHead) {
        for uncle in &head.uncles {
            let removed = self.hash_to_number.remove(uncle);
            if let Some(removed) = removed {
                self.number_to_hash.remove(&removed);
            }
        }

        self.hash_to_number
            .insert(head.block.hash, head.block.number);
        self.number_to_hash
            .insert(head.block.number, head.block.hash);

        let number = head.block.number;
        self.chain_head_broadcast_tx.write(head.block);

        with_metric(&METRICS.chain_head, &[&self.network], |g| {
            g.set(number as i64)
        });
    }

    async fn handle_block(&mut self, block: BlockPointer) {
        self.hash_to_number.insert(block.hash.clone(), block.number);
        self.number_to_hash.insert(block.number, block.hash.clone());

        self.complete_block_requests(Ok(block.clone()));

        let capacity = self.pending_requirements.len();
        for mut entry in mem::replace(&mut self.pending_requirements, Vec::with_capacity(capacity))
        {
            for resolved in entry
                .unresolved
                .iter()
                .filter(|u| u.matches(&block))
                .cloned()
                .collect::<Vec<UnresolvedBlock>>()
            {
                entry.unresolved.remove(&resolved);
                entry.requirements.resolved.insert(block.clone());
            }
            if entry.unresolved.is_empty() {
                let _ = entry.tx.send(Ok(entry.requirements));
                continue;
            }
            debug_assert!(entry.requirements.resolved());
            self.pending_requirements.push(entry);
        }
    }

    async fn handle_err(&mut self, unresolved: UnresolvedBlock) {
        self.complete_block_requests(Err(unresolved.clone()));

        // Cancel pending requirements requests depending on the unresolved block.
        let capacity = self.pending_requirements.len();
        for entry in mem::replace(&mut self.pending_requirements, Vec::with_capacity(capacity)) {
            if entry.unresolved.contains(&unresolved) {
                let _ = entry.tx.send(Err(unresolved.clone()));
                continue;
            }
            self.pending_requirements.push(entry);
        }
    }

    async fn handle_block_request(
        &mut self,
        number: u64,
        tx: oneshot::Sender<Result<BlockPointer, UnresolvedBlock>>,
    ) {
        match self.number_to_hash.get(&number).cloned() {
            Some(hash) => {
                with_metric(&METRICS.block_cache_hit, &[&self.network], |c| c.inc());
                let _ = tx.send(Ok(BlockPointer { number, hash }));
            }
            None => {
                with_metric(&METRICS.block_cache_miss, &[&self.network], |c| c.inc());
                let _ = self.client_tx.send(UnresolvedBlock::WithNumber(number));
                self.pending_block.entry(number).or_default().push(tx);
            }
        }
    }

    fn complete_block_requests(&mut self, result: Result<BlockPointer, UnresolvedBlock>) {
        let number = match &result {
            Ok(block) => block.number,
            Err(UnresolvedBlock::WithNumber(number)) => *number,
            Err(UnresolvedBlock::WithHash(_)) => return,
        };
        if let Entry::Occupied(entry) = self.pending_block.entry(number) {
            for tx in entry.remove() {
                let _ = tx.send(result.clone());
            }
        }
    }

    async fn handle_block_requirements(
        &mut self,
        constraints: BTreeSet<BlockConstraint>,
        tx: oneshot::Sender<Result<BlockRequirements, UnresolvedBlock>>,
    ) {
        let mut requirements = BlockRequirements {
            constraints,
            resolved: BTreeSet::new(),
        };
        let mut unresolved = BTreeSet::new();
        for constraint in &requirements.constraints {
            let unresolved_block = match constraint.clone().into_unresolved() {
                Some(unresolved_block) => unresolved_block,
                None => continue,
            };
            let result = match &unresolved_block {
                UnresolvedBlock::WithHash(h) => self.hash_to_number.get(&h).map(|n| BlockPointer {
                    hash: h.clone(),
                    number: *n,
                }),
                UnresolvedBlock::WithNumber(n) => {
                    self.number_to_hash.get(n).map(|h| BlockPointer {
                        hash: h.clone(),
                        number: *n,
                    })
                }
            };
            match result {
                Some(block) => {
                    with_metric(&METRICS.block_cache_hit, &[&self.network], |c| c.inc());
                    requirements.resolved.insert(block);
                }
                None => {
                    with_metric(&METRICS.block_cache_miss, &[&self.network], |c| c.inc());
                    let _ = self.client_tx.send(unresolved_block.clone());
                    unresolved.insert(unresolved_block);
                }
            };
        }

        if requirements.resolved() {
            let _ = tx.send(Ok(requirements));
            return;
        }

        self.pending_requirements.push(PendingRequirementsRequest {
            requirements,
            unresolved,
            tx,
        });
    }
}
