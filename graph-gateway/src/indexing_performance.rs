use std::{collections::HashMap, ops::Deref, time::Duration};

use alloy_primitives::{Address, BlockNumber};
use parking_lot::RwLock;
use thegraph_core::types::DeploymentId;
use tokio::{
    self,
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};

use crate::network::{internal::NetworkTopologySnapshot, IndexingId};

#[derive(Default)]
pub struct Snapshot {
    pub response: indexer_selection::Performance,
    pub latest_block: Option<BlockNumber>,
}

#[derive(Clone)]
pub struct IndexingPerformance {
    data: &'static DoubleBuffer,
    msgs: mpsc::UnboundedSender<Feedback>,
}

struct Feedback {
    indexer: Address,
    deployment: DeploymentId,
    success: bool,
    latency_ms: u16,
    latest_block: Option<BlockNumber>,
}

impl IndexingPerformance {
    pub fn new(network: watch::Receiver<NetworkTopologySnapshot>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let data: &'static DoubleBuffer = Box::leak(Box::default());
        Actor::spawn(data, rx, network);
        Self { data, msgs: tx }
    }

    pub fn latest(&self) -> impl Deref<Target = HashMap<(Address, DeploymentId), Snapshot>> + '_ {
        loop {
            // This is guaranteed to only move forward in time, and is almost guaranteed to acquire
            // the lock "immediately". These guarantees come from the invariant that there is a
            // single writer, and it can only be in a few possible states.
            for unlocked in &self.data.0 {
                if let Some(data) = unlocked.try_read() {
                    return data;
                }
            }
        }
    }

    pub fn feedback(
        &self,
        indexer: Address,
        deployment: DeploymentId,
        success: bool,
        latency_ms: u16,
        latest_block: Option<BlockNumber>,
    ) {
        self.msgs
            .send(Feedback {
                indexer,
                deployment,
                success,
                latency_ms,
                latest_block,
            })
            .unwrap();
    }
}

#[derive(Default)]
struct DoubleBuffer([RwLock<HashMap<(Address, DeploymentId), Snapshot>>; 2]);

struct Actor {
    data: &'static DoubleBuffer,
}

impl Actor {
    fn spawn(
        data: &'static DoubleBuffer,
        mut messages: mpsc::UnboundedReceiver<Feedback>,
        mut network: watch::Receiver<NetworkTopologySnapshot>,
    ) {
        let mut actor = Self { data };
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::spawn(async move {
            let batch_limit = 32;
            let mut msg_buf = Vec::with_capacity(batch_limit);
            loop {
                tokio::select! {
                    _ = timer.tick() => actor.decay(),
                    _ = messages.recv_many(&mut msg_buf, batch_limit) => actor.handle_msgs(&mut msg_buf),
                    _ = network.changed() => actor.handle_network(&network.borrow()),
                }
            }
        });
    }

    fn decay(&mut self) {
        for unlocked in &self.data.0 {
            for snapshot in unlocked.write().values_mut() {
                snapshot.response.decay();
            }
        }
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<Feedback>) {
        for unlocked in &self.data.0 {
            let mut locked = unlocked.write();
            for Feedback {
                indexer,
                deployment,
                success,
                latency_ms,
                latest_block,
            } in msgs.drain(..)
            {
                let snapshot = locked.entry((indexer, deployment)).or_default();
                snapshot.response.feedback(success, latency_ms);
                snapshot.latest_block = match (snapshot.latest_block, latest_block) {
                    (None, block) => block,
                    (Some(a), Some(b)) if b > a => Some(b),
                    (Some(a), _) => Some(a),
                };
            }
        }
        debug_assert!(msgs.is_empty());
    }

    fn handle_network(&mut self, network: &NetworkTopologySnapshot) {
        let progress: HashMap<IndexingId, BlockNumber> = network
            .deployments()
            .iter()
            .flat_map(|(_, result)| result.iter().flat_map(|d| &d.indexings))
            .flat_map(|(id, indexing)| indexing.iter().map(|i| (*id, i.progress.latest_block)))
            .collect();

        for unlocked in &self.data.0 {
            let mut locked = unlocked.write();
            for (indexing, latest_block) in progress.iter() {
                let snapshot = locked
                    .entry((indexing.indexer, indexing.deployment))
                    .or_default();
                if *latest_block >= snapshot.latest_block.unwrap_or(0) {
                    snapshot.latest_block = Some(*latest_block);
                };
            }
        }
    }
}
