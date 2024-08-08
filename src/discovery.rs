mod indexer;
pub mod network_subgraph;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::BlockNumber;
use anyhow::Context as _;
use custom_debug::CustomDebug;
use futures::{stream::FuturesUnordered, StreamExt};
pub use indexer::Versions;
use ipnetwork::IpNetwork;
use network_subgraph::TrustedIndexer;
use thegraph_core::types::{AllocationId, DeploymentId, IndexerId, SubgraphId};
use tokio::sync::watch;
use url::Url;

use crate::{errors::UnavailableReason, indexer_client::IndexerClient};

#[derive(Default)]
pub struct Topology {
    pub subgraphs: HashMap<SubgraphId, Subgraph>,
    pub deployments: HashMap<DeploymentId, Arc<Deployment>>,
}

pub struct Subgraph {
    pub id: SubgraphId,
    pub versions: Vec<Arc<Deployment>>,
}

#[derive(Debug)]
pub struct Deployment {
    pub id: DeploymentId,
    pub chain: String,
    pub start_block: BlockNumber,
    pub allocations: Vec<Allocation>,
}

#[derive(Debug)]
pub struct Allocation {
    pub id: AllocationId,
    pub indexer: Arc<Indexer>,
    pub tokens: u128,
    pub status: IndexingStatus,
    // pub cost_model: Option<CostModel>,
}

#[derive(CustomDebug)]
pub struct Indexer {
    pub id: IndexerId,
    /// guaranteed to be a valid base URL, with a scheme of either HTTP or HTTPS
    /// ref: df8e647b-1e6e-422a-8846-dc9ee7e0dcc2
    #[debug(with = std::fmt::Display::fmt)]
    pub url: Url,
    pub staked_tokens: u128,
    pub tap_support: bool,
}

#[derive(Debug)]
pub struct IndexingStatus {
    pub latest_block: BlockNumber,
    pub min_block: Option<BlockNumber>,
}

pub async fn spawn(
    indexer_client: IndexerClient,
    trusted_indexers: Vec<TrustedIndexer>,
    http: reqwest::Client,
    indexer_blocklist: HashSet<IndexerId>,
    ip_blocklist: HashSet<IpNetwork>,
    min_versions: Versions,
) -> watch::Receiver<Topology> {
    let mut state = State {
        network_subgraph: network_subgraph::Client {
            client: indexer_client,
            indexers: trusted_indexers,
            latest_block: None,
            page_size: 500,
        },
        indexer_validator: indexer::Validator::new(
            http,
            indexer_blocklist,
            ip_blocklist,
            min_versions,
        ),
        indexer_cache: Default::default(),
        indexing_status: Default::default(),
    };
    let (mut tx, mut rx) = watch::channel(Topology::default());
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(30));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if let Err(discovery_err) = state.update(&mut tx).await {
                tracing::error!(discovery_err = format!("{:#}", discovery_err));
            }
        }
    });

    rx.wait_for(|topology| !topology.subgraphs.is_empty())
        .await
        .unwrap();
    rx
}

struct State {
    network_subgraph: network_subgraph::Client,
    indexer_validator: indexer::Validator,
    // This cache retains entries indefinitely, so it should only contain results that should
    // persist until gateway restart.
    indexer_cache: HashMap<IndexerId, Result<Arc<Indexer>, UnavailableReason>>,
    indexing_status: HashMap<AllocationId, IndexingStatus>,
}

impl State {
    async fn update(&mut self, tx: &mut watch::Sender<Topology>) -> anyhow::Result<()> {
        // TODO
        // let subgraphs = self
        //     .network_subgraph
        //     .fetch()
        //     .await
        //     .context("poll network subgraph")?;
        let subgraphs: Vec<network_subgraph::types::Subgraph> = {
            let text = std::fs::read_to_string("../secrets/test.json").unwrap();
            serde_json::from_str(&text).unwrap()
        };

        let indexers: Vec<&network_subgraph::types::Indexer> = subgraphs
            .iter()
            .flat_map(|s| &s.versions)
            .flat_map(|v| &v.subgraph_deployment.allocations)
            .map(|a| (a.indexer.id, &a.indexer))
            .collect::<HashMap<IndexerId, &network_subgraph::types::Indexer>>() // dedup
            .into_values()
            .collect();
        let indexers: HashMap<IndexerId, Result<Arc<Indexer>, UnavailableReason>> =
            self.resolve_indexers(&indexers).await;
        let reachable_indexers: Vec<&Indexer> = indexers
            .iter()
            .filter_map(|(_, result)| result.as_deref().ok())
            .collect();

        // let reachable_indexer_allocations:  = reachable_indexers

        #[rustfmt::skip]
        tracing::info!(
            subgraphs = subgraphs.len(),
            indexers = indexers.len(),
            reachable_indexers = reachable_indexers.len(),
            blocked_indexers = indexers.iter().filter(|(_, result)| matches!(&result, Err(UnavailableReason::Blocked))).count(),
        );
        todo!();
    }

    async fn resolve_indexers(
        &mut self,
        info: &[&network_subgraph::types::Indexer],
    ) -> HashMap<IndexerId, Result<Arc<Indexer>, UnavailableReason>> {
        let mut indexers: HashMap<IndexerId, Result<Arc<Indexer>, UnavailableReason>> =
            Default::default();
        let unresolved = FuturesUnordered::new();
        for info in info {
            if let Some(result) = self.indexer_cache.get(&info.id) {
                // Invalidate cache entry if URL has changed.
                if let Ok(indexer) = &result {
                    let new_url = info.url.as_deref().unwrap_or_default();
                    let cached_url = indexer.url.as_str();
                    if new_url == cached_url {
                        indexers.insert(info.id, result.clone());
                        continue;
                    }
                    tracing::info!(
                        indexer = %info.id,
                        cached_url,
                        new_url,
                        "invalidate indexer cache"
                    );
                    self.indexer_cache.remove(&info.id);
                }
            }
            unresolved.push(async { (info.id, self.indexer_validator.resolve(info).await) });
        }
        let results = unresolved.collect::<Vec<_>>().await;
        tracing::warn!(results = results.len());
        for (id, result) in results {
            if matches!(&result, Ok(_) | Err(UnavailableReason::Blocked)) {
                self.indexer_cache.insert(id, result.clone());
            }
            indexers.insert(id, result);
        }
        indexers
    }
}
