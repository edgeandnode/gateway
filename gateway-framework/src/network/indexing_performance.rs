use std::{collections::HashMap, ops::Deref, time::Duration};

use alloy_primitives::BlockNumber;
use eventuals::{Closed, Eventual, Ptr};
use gateway_common::types::Indexing;
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
    time::{self, MissedTickBehavior},
};

use crate::network::discovery::Status;

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
        indexing: Indexing,
        success: bool,
        latency_ms: u16,
        latest_block: Option<BlockNumber>,
    },
    Flush(oneshot::Sender<()>),
}

impl IndexingPerformance {
    #[allow(clippy::new_without_default)]
    pub fn new(indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let data: &'static DoubleBuffer = Box::leak(Box::default());
        Actor::spawn(data, rx, indexing_statuses);
        Self { data, msgs: tx }
    }

    pub fn latest(&'_ self) -> impl '_ + Deref<Target = HashMap<Indexing, Snapshot>> {
        loop {
            // This is guaranteed to only move forward in time, and is almost guaranteed to acquire
            // the lock "immediately". These guarantees come from the invariant that there is a
            // single writer and it can only be in a few possible states.
            for unlocked in &self.data.0 {
                if let Ok(data) = unlocked.try_read() {
                    return data;
                }
            }
        }
    }

    pub fn feedback(
        &self,
        indexing: Indexing,
        success: bool,
        latency_ms: u16,
        latest_block: Option<BlockNumber>,
    ) {
        self.msgs
            .send(Msg::Feedback {
                indexing,
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
struct DoubleBuffer([RwLock<HashMap<Indexing, Snapshot>>; 2]);

struct Actor {
    data: &'static DoubleBuffer,
}

impl Actor {
    fn spawn(
        data: &'static DoubleBuffer,
        mut msgs: mpsc::UnboundedReceiver<Msg>,
        indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    ) {
        let mut actor = Self { data };
        let mut statuses = indexing_statuses.subscribe();
        let mut timer = time::interval(Duration::from_secs(1));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::spawn(async move {
            let batch_limit = 32;
            let mut msg_buf = Vec::with_capacity(batch_limit);
            loop {
                select! {
                    _ = timer.tick() => actor.decay().await,
                    _ = msgs.recv_many(&mut msg_buf, batch_limit) => actor.handle_msgs(&mut msg_buf).await,
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
                        indexing,
                        success,
                        latency_ms,
                        latest_block,
                    } => {
                        let snapshot = locked.entry(indexing).or_default();
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

    async fn handle_statuses(&mut self, statuses: Result<Ptr<HashMap<Indexing, Status>>, Closed>) {
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
                if status.block >= snapshot.latest_block.unwrap_or(0) {
                    snapshot.latest_block = Some(status.block);
                };
            }
        }
    }
}
