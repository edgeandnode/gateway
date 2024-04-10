use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use alloy_primitives::Address;
use gateway_framework::{blocks::Block, chain::Chain, metrics::METRICS};
use tokio::{
    select, spawn,
    sync::{mpsc, RwLock, RwLockReadGuard},
    time::interval,
};

#[derive(Clone)]
pub struct ChainReader {
    tx: mpsc::UnboundedSender<Msg>,
    chain: &'static RwLock<Chain>,
}

impl ChainReader {
    pub async fn read(&self) -> RwLockReadGuard<'_, Chain> {
        self.chain.read().await
    }

    pub fn notify(&self, block: Block, indexer: Address) {
        let _ = self.tx.send(Msg { block, indexer });
    }
}

pub struct Chains {
    data: RwLock<HashMap<String, ChainReader>>,
    aliases: BTreeMap<String, String>,
}

impl Chains {
    pub fn new(aliases: BTreeMap<String, String>) -> Self {
        Self {
            data: Default::default(),
            aliases,
        }
    }

    pub async fn chain(&self, name: &str) -> ChainReader {
        let name = self.aliases.get(name).map(|a| a.as_str()).unwrap_or(name);
        {
            let reader = self.data.read().await;
            if let Some(chain) = reader.get(name) {
                return chain.clone();
            }
        }
        {
            let mut writer = self.data.write().await;
            writer
                .entry(name.to_string())
                .or_insert_with(|| Actor::spawn(name.to_string()))
                .clone()
        }
    }
}

struct Msg {
    block: Block,
    indexer: Address,
}

struct Actor;

impl Actor {
    pub fn spawn(chain_name: String) -> ChainReader {
        let chain: &'static RwLock<Chain> = Box::leak(Box::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn(async move {
            let mut msgs: Vec<Msg> = Default::default();
            let mut timer = interval(Duration::from_secs(1));
            loop {
                select! {
                    _ = rx.recv_many(&mut msgs, 32) => Self::handle_msgs(chain, &mut msgs).await,
                    _ = timer.tick() => {
                        let blocks_per_minute = chain.read().await.blocks_per_minute();
                        METRICS
                            .blocks_per_minute
                            .with_label_values(&[&chain_name])
                            .set(blocks_per_minute as i64);
                    },
                }
            }
        });
        ChainReader { tx, chain }
    }

    async fn handle_msgs(chain: &RwLock<Chain>, msgs: &mut Vec<Msg>) {
        {
            let reader = chain.read().await;
            msgs.retain(|Msg { block, indexer }| reader.should_insert(block, indexer));
        }
        {
            let mut writer = chain.write().await;
            for Msg { block, indexer } in msgs.drain(..) {
                if writer.should_insert(&block, &indexer) {
                    writer.insert(block, indexer);
                }
            }
        }
        debug_assert!(msgs.is_empty());
    }
}
