use std::{collections::HashMap, ops::Deref, time::Duration};

use alloy_primitives::{Address, BlockNumber};
use eventuals::{Closed, Eventual, Ptr};
use thegraph_core::types::DeploymentId;
use tokio::{
    self,
    sync::{mpsc, oneshot, RwLock},
    time::MissedTickBehavior,
};

#[derive(Clone)]
pub struct Status {
    /// The latest reported block number
    pub latest_block: BlockNumber,
}

#[derive(Default)]
pub struct Snapshot {
    pub response: indexer_selection::Performance,
    pub latest_block: Option<BlockNumber>,
}

#[derive(Clone)]
pub struct IndexingPerformance {
    data: &'static DoubleBuffer,
    msgs: mpsc::UnboundedSender<Msg>,
}

pub enum Msg {
    Feedback {
        indexer: Address,
        deployment: DeploymentId,
        success: bool,
        latency_ms: u16,
        latest_block: Option<BlockNumber>,
    },
    Flush(oneshot::Sender<()>),
}

impl IndexingPerformance {
    #[allow(clippy::new_without_default)]
    pub fn new(indexing_statuses: Eventual<Ptr<HashMap<(Address, DeploymentId), Status>>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let data: &'static DoubleBuffer = Box::leak(Box::default());
        Actor::spawn(data, rx, indexing_statuses);
        Self { data, msgs: tx }
    }

    pub fn latest(&self) -> impl Deref<Target = HashMap<(Address, DeploymentId), Snapshot>> + '_ {
        loop {
            // This is guaranteed to only move forward in time, and is almost guaranteed to acquire
            // the lock "immediately". These guarantees come from the invariant that there is a
            // single writer, and it can only be in a few possible states.
            for unlocked in &self.data.0 {
                if let Ok(data) = unlocked.try_read() {
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
            .send(Msg::Feedback {
                indexer,
                deployment,
                success,
                latency_ms,
                latest_block,
            })
            .unwrap();
    }

    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        self.msgs.send(Msg::Flush(tx)).unwrap();
        let _ = rx.await;
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
        mut messages: mpsc::UnboundedReceiver<Msg>,
        indexing_statuses: Eventual<Ptr<HashMap<(Address, DeploymentId), Status>>>,
    ) {
        let mut actor = Self { data };
        let mut statuses = indexing_statuses.subscribe();
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::spawn(async move {
            let batch_limit = 32;
            let mut msg_buf = Vec::with_capacity(batch_limit);
            loop {
                tokio::select! {
                    _ = timer.tick() => actor.decay().await,
                    _ = messages.recv_many(&mut msg_buf, batch_limit) => actor.handle_msgs(&mut msg_buf).await,
                    statuses = statuses.next() => actor.handle_statuses(statuses).await,
                }
            }
        });
    }

    async fn decay(&mut self) {
        for unlocked in &self.data.0 {
            for snapshot in unlocked.write().await.values_mut() {
                snapshot.response.decay();
            }
        }
    }

    async fn handle_msgs(&mut self, msgs: &mut Vec<Msg>) {
        for unlocked in &self.data.0 {
            let mut locked = unlocked.write().await;
            for msg in msgs.iter() {
                match msg {
                    Msg::Flush(_) => (),
                    &Msg::Feedback {
                        indexer,
                        deployment,
                        success,
                        latency_ms,
                        latest_block,
                    } => {
                        let snapshot = locked.entry((indexer, deployment)).or_default();
                        snapshot.response.feedback(success, latency_ms);
                        snapshot.latest_block = match (snapshot.latest_block, latest_block) {
                            (None, block) => block,
                            (Some(a), Some(b)) if b > a => Some(b),
                            (Some(a), _) => Some(a),
                        };
                    }
                }
            }
        }
        for msg in msgs.drain(..) {
            if let Msg::Flush(notify) = msg {
                if notify.send(()).is_err() {
                    tracing::error!("flush notify failed");
                };
            }
        }
        debug_assert!(msgs.is_empty());
    }

    #[allow(clippy::type_complexity)]
    async fn handle_statuses(
        &mut self,
        statuses: Result<Ptr<HashMap<(Address, DeploymentId), Status>>, Closed>,
    ) {
        let statuses = match statuses {
            Ok(statuses) => statuses,
            Err(_) => {
                tracing::error!("indexing statuses closed");
                return;
            }
        };
        for unlocked in &self.data.0 {
            let mut locked = unlocked.write().await;
            for (indexing, status) in statuses.iter() {
                let snapshot = locked.entry(*indexing).or_default();
                if status.latest_block >= snapshot.latest_block.unwrap_or(0) {
                    snapshot.latest_block = Some(status.latest_block);
                };
            }
        }
    }
}
