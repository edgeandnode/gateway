use std::collections::{HashMap, HashSet};

use futures::StreamExt as _;
use rdkafka::Message;
use thegraph_core::{DeploymentId, ProofOfIndexing, alloy::primitives::Address};
use tokio::sync::watch;

use crate::{KafkaConsumer, config::BlocklistEntry};

#[derive(Clone)]
pub struct Blocklist {
    pub blocklist: watch::Receiver<Vec<BlocklistEntry>>,
    pub poi: watch::Receiver<HashMap<DeploymentId, Vec<(u64, ProofOfIndexing)>>>,
    pub indexer: watch::Receiver<HashMap<Address, HashSet<DeploymentId>>>,
}

impl Blocklist {
    pub fn spawn(init: Vec<BlocklistEntry>, consumer: KafkaConsumer) -> Self {
        let (blocklist_tx, blocklist_rx) = watch::channel(Default::default());
        let (poi_tx, poi_rx) = watch::channel(Default::default());
        let (indexer_tx, indexer_rx) = watch::channel(Default::default());
        let mut actor = Actor {
            blocklist: blocklist_tx,
            poi: poi_tx,
            indexer: indexer_tx,
        };
        for entry in init {
            actor.add_entry(entry);
        }
        tokio::spawn(async move {
            actor.run(consumer).await;
        });
        Self {
            blocklist: blocklist_rx,
            poi: poi_rx,
            indexer: indexer_rx,
        }
    }
}

struct Actor {
    blocklist: watch::Sender<Vec<BlocklistEntry>>,
    poi: watch::Sender<HashMap<DeploymentId, Vec<(u64, ProofOfIndexing)>>>,
    indexer: watch::Sender<HashMap<Address, HashSet<DeploymentId>>>,
}

impl Actor {
    async fn run(&mut self, consumer: KafkaConsumer) {
        let consumer = match consumer.stream_consumer("gateway_blocklist") {
            Ok(consumer) => consumer,
            Err(blocklist_err) => {
                tracing::error!(%blocklist_err);
                return;
            }
        };

        let mut records: HashMap<String, BlocklistEntry> = Default::default();
        let mut stream = consumer.stream();
        while let Some(msg) = stream.next().await {
            let msg = match msg {
                Ok(msg) => msg,
                Err(blocklist_recv_error) => {
                    tracing::error!(%blocklist_recv_error);
                    continue;
                }
            };
            let key = match msg.key_view::<str>() {
                Some(Ok(key)) => key,
                result => {
                    tracing::error!("invalid key: {result:?}");
                    continue;
                }
            };
            match msg.payload().map(serde_json::from_slice::<BlocklistEntry>) {
                Some(Ok(entry)) => {
                    records.insert(key.to_string(), entry.clone());
                    self.add_entry(entry);
                }
                None => {
                    let entry = records.remove(key);
                    if let Some(entry) = entry {
                        self.remove_entry(&entry);
                    }
                }
                Some(Err(blocklist_deserialize_err)) => {
                    tracing::error!(%blocklist_deserialize_err);
                }
            };
        }
        tracing::error!("blocklist consumer stopped");
    }

    fn add_entry(&mut self, entry: BlocklistEntry) {
        match entry {
            BlocklistEntry::Poi {
                deployment,
                block,
                public_poi,
                ..
            } => {
                self.poi.send_modify(move |blocklist| {
                    blocklist
                        .entry(deployment)
                        .or_default()
                        .push((block, public_poi.into()));
                });
            }
            BlocklistEntry::Other {
                deployment,
                indexer,
                ..
            } => {
                self.indexer.send_modify(move |blocklist| {
                    blocklist.entry(indexer).or_default().insert(deployment);
                });
            }
        };
        self.blocklist
            .send_modify(move |blocklist| blocklist.push(entry));
    }

    fn remove_entry(&mut self, entry: &BlocklistEntry) {
        match entry {
            BlocklistEntry::Poi {
                deployment,
                block,
                public_poi,
                ..
            } => {
                self.poi.send_modify(|blocklist| {
                    if let Some(entry) = blocklist.get_mut(deployment) {
                        entry.retain(|value| &(*block, (*public_poi).into()) != value);
                    }
                });
            }
            BlocklistEntry::Other {
                deployment,
                indexer,
                ..
            } => {
                self.indexer.send_modify(|blocklist| {
                    if let Some(entry) = blocklist.get_mut(indexer) {
                        entry.remove(deployment);
                    }
                });
            }
        };
        fn matching(a: &BlocklistEntry, b: &BlocklistEntry) -> bool {
            match (a, b) {
                (
                    BlocklistEntry::Poi {
                        deployment,
                        public_poi,
                        block,
                    },
                    BlocklistEntry::Poi {
                        deployment: deployment_,
                        public_poi: public_poi_,
                        block: block_,
                    },
                ) => {
                    (deployment == deployment_) && (public_poi == public_poi_) && (block == block_)
                }
                (
                    BlocklistEntry::Other {
                        indexer,
                        deployment,
                    },
                    BlocklistEntry::Other {
                        indexer: indexer_,
                        deployment: deployment_,
                    },
                ) => (indexer == indexer_) && (deployment == deployment_),
                _ => false,
            }
        }
        self.blocklist
            .send_modify(|blocklist| blocklist.retain(|value| !matching(entry, value)));
    }
}
