//! Multi-Chain Head Tracking
//!
//! Manages chain head tracking across all indexed blockchains.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐
//! │   Chains     │  Main container, maps chain name → ChainReader
//! │ (with        │
//! │  aliases)    │
//! └──────┬───────┘
//!        │
//!        ▼
//! ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
//! │ ChainReader  │     │ ChainReader  │     │ ChainReader  │
//! │ "ethereum"   │     │ "polygon"    │     │ "arbitrum"   │
//! └──────┬───────┘     └──────────────┘     └──────────────┘
//!        │
//!        ▼
//! ┌──────────────┐
//! │   Actor      │  Background task updating Chain state
//! │ (per chain)  │
//! └──────────────┘
//! ```
//!
//! # Chain Aliases
//!
//! Supports aliases like `"mainnet"` → `"ethereum"` configured in gateway config.
//!
//! # Usage
//!
//! ```ignore
//! let chains = Chains::new(aliases);
//! let eth = chains.chain("ethereum");
//!
//! // Report a block from an indexer
//! eth.notify(block, indexer);
//!
//! // Read chain state
//! let chain = eth.read();
//! let head = chain.latest();
//! ```

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use parking_lot::{RwLock, RwLockReadGuard};
use thegraph_core::IndexerId;
use tokio::{
    select, spawn,
    sync::mpsc,
    time::{MissedTickBehavior, interval},
};

use crate::{blocks::Block, chain::Chain, metrics::METRICS};

#[derive(Clone)]
pub struct ChainReader {
    tx: mpsc::UnboundedSender<Msg>,
    chain: &'static RwLock<Chain>,
}

impl ChainReader {
    pub fn read(&self) -> RwLockReadGuard<'_, Chain> {
        self.chain.read()
    }

    pub fn notify(&self, block: Block, indexer: IndexerId) {
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

    pub fn chain(&self, name: &str) -> ChainReader {
        let name = self.aliases.get(name).map(|a| a.as_str()).unwrap_or(name);
        {
            let reader = self.data.read();
            if let Some(chain) = reader.get(name) {
                return chain.clone();
            }
        }
        {
            let mut writer = self.data.write();
            writer
                .entry(name.to_string())
                .or_insert_with(|| Actor::spawn(name.to_string()))
                .clone()
        }
    }
}

struct Msg {
    block: Block,
    indexer: IndexerId,
}

struct Actor;

impl Actor {
    pub fn spawn(chain_name: String) -> ChainReader {
        let chain: &'static RwLock<Chain> = Box::leak(Box::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn(async move {
            let mut msgs: Vec<Msg> = Default::default();
            let mut timer = interval(Duration::from_secs(1));
            timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                select! {
                    _ = rx.recv_many(&mut msgs, 32) => Self::handle_msgs(chain, &mut msgs),
                    _ = timer.tick() => {
                        let blocks_per_minute = chain.read().blocks_per_minute();
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

    fn handle_msgs(chain: &RwLock<Chain>, msgs: &mut Vec<Msg>) {
        {
            let reader = chain.read();
            msgs.retain(|Msg { block, indexer }| reader.should_insert(block, indexer));
        }
        {
            let mut writer = chain.write();
            for Msg { block, indexer } in msgs.drain(..) {
                if writer.should_insert(&block, &indexer) {
                    writer.insert(block, indexer);
                }
            }
        }
        debug_assert!(msgs.is_empty());
    }
}
