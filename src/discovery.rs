mod indexer;
pub mod network_subgraph;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Context as _;
use custom_debug::CustomDebug;
use futures::{stream::FuturesUnordered, StreamExt};
pub use indexer::Versions;
use ipnetwork::IpNetwork;
use itertools::Itertools;
use network_subgraph::TrustedIndexer;
use thegraph_core::types::IndexerId;
use tokio::sync::watch;
use url::Url;

use crate::{errors::UnavailableReason, indexer_client::IndexerClient};

#[derive(Default)]
pub struct Topology {
    ready: bool,
}

#[derive(CustomDebug)]
pub struct Indexer {
    pub id: IndexerId,
    /// guaranteed to be a valid base URL, with a scheme of either HTTP or HTTPS
    /// ref: df8e647b-1e6e-422a-8846-dc9ee7e0dcc2
    #[debug(with = std::fmt::Display::fmt)]
    pub url: Url,
    pub staked_tokens: u128,
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

    rx.wait_for(|topology| topology.ready).await.unwrap();
    rx
}

struct State {
    network_subgraph: network_subgraph::Client,
    indexer_validator: indexer::Validator,
    // This cache retains entries indefinitely, so it should only contain results that should
    // persist until gateway restart.
    indexer_cache: HashMap<IndexerId, Result<Arc<Indexer>, UnavailableReason>>,
}

impl State {
    async fn update(&mut self, tx: &mut watch::Sender<Topology>) -> anyhow::Result<()> {
        // TODO
        let subgraphs = self
            .network_subgraph
            .fetch()
            .await
            .context("poll network subgraph")?;
        // let subgraphs: Vec<network_subgraph::types::Subgraph> = {
        //     let text = std::fs::read_to_string("../secrets/test.json").unwrap();
        //     serde_json::from_str(&text).unwrap()
        // };

        let indexers_info = subgraphs
            .iter()
            .flat_map(|s| &s.versions)
            .flat_map(|v| &v.subgraph_deployment.allocations)
            .map(|a| &a.indexer)
            .collect_vec();

        let indexers: HashMap<IndexerId, Result<Arc<Indexer>, UnavailableReason>> =
            self.resolve_indexers(&indexers_info).await;

        tracing::info!(subgraphs = subgraphs.len(), indexers = indexers.len(),);
        let results = indexers
            .iter()
            .map(|(id, result)| (id, result.as_ref().map(|_| ())))
            .collect_vec();
        tracing::warn!("{:#?}", results);

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
        for (id, result) in unresolved.collect::<Vec<_>>().await {
            if matches!(&result, Ok(_) | Err(UnavailableReason::Blocked)) {
                self.indexer_cache.insert(id, result.clone());
            }
            indexers.insert(id, result);
        }
        indexers
    }
}
