use std::collections::{HashMap, HashSet};

use thegraph_core::{alloy::primitives::Address, DeploymentId, ProofOfIndexing};
use tokio::sync::watch;

use crate::config::BlocklistEntry;

#[derive(Clone)]
pub struct Blocklist {
    pub blocklist: watch::Receiver<Vec<BlocklistEntry>>,
    pub poi: watch::Receiver<HashMap<DeploymentId, Vec<(u64, ProofOfIndexing)>>>,
    pub indexer: watch::Receiver<HashMap<Address, HashSet<DeploymentId>>>,
}

impl Blocklist {
    pub fn spawn(init: Vec<BlocklistEntry>) -> Self {
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
            actor.run().await;
        });
        Self {
            blocklist: blocklist_rx,
            poi: poi_rx,
            indexer: indexer_rx,
        }
    }
}

struct Actor {
    pub blocklist: watch::Sender<Vec<BlocklistEntry>>,
    pub poi: watch::Sender<HashMap<DeploymentId, Vec<(u64, ProofOfIndexing)>>>,
    pub indexer: watch::Sender<HashMap<Address, HashSet<DeploymentId>>>,
}

impl Actor {
    async fn run(&mut self) {
        todo!();
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
}
