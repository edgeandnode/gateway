use gateway_common::types::Indexing;
use indexer_selection::Performance;
use std::{collections::HashMap, ops::Deref, time::Duration};
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
    time,
};

#[derive(Clone)]
pub struct IndexingPerformance {
    data: &'static DoubleBuffer,
    msgs: mpsc::UnboundedSender<Msg>,
}

pub enum Msg {
    Feedback {
        indexing: Indexing,
        success: bool,
        latency_ms: u32,
    },
    Flush(oneshot::Sender<()>),
}

impl IndexingPerformance {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let data: &'static DoubleBuffer = Box::leak(Box::default());
        Actor::spawn(data, rx);
        Self { data, msgs: tx }
    }

    pub fn latest(&'_ self) -> impl '_ + Deref<Target = HashMap<Indexing, Performance>> {
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

    pub fn feedback(&self, indexing: Indexing, success: bool, latency_ms: u32) {
        self.msgs
            .send(Msg::Feedback {
                indexing,
                success,
                latency_ms,
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
struct DoubleBuffer([RwLock<HashMap<Indexing, Performance>>; 2]);

struct Actor {
    data: &'static DoubleBuffer,
}

impl Actor {
    fn spawn(data: &'static DoubleBuffer, mut msgs: mpsc::UnboundedReceiver<Msg>) {
        let mut actor = Self { data };
        let mut timer = time::interval(Duration::from_secs(1));
        tokio::spawn(async move {
            let batch_limit = 32;
            let mut msg_buf = Vec::with_capacity(batch_limit);
            loop {
                select! {
                    _ = timer.tick() => actor.decay().await,
                    _ = msgs.recv_many(&mut msg_buf, batch_limit) => actor.handle_msgs(&mut msg_buf).await,
                }
            }
        });
    }

    async fn decay(&mut self) {
        for unlocked in &self.data.0 {
            for perf in unlocked.write().await.values_mut() {
                perf.decay();
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
                    } => {
                        locked
                            .entry(indexing)
                            .or_default()
                            .feedback(success, latency_ms);
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
}
