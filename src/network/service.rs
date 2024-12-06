//! The [`NetworkService`] is a `graph-gateway` specific abstraction layer providing a
//! simplified interface for resolving the subgraph-specific information required by the
//! query processing pipeline

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::{
    alloy::primitives::{Address, BlockNumber},
    DeploymentId, SubgraphId,
};
use tokio::{sync::watch, time::MissedTickBehavior};

use super::{
    cost_model::CostModelResolver,
    errors::{DeploymentError, SubgraphError},
    host_filter::HostFilter,
    indexer_indexing_poi_blocklist::PoiBlocklist,
    indexer_indexing_poi_resolver::PoiResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    internal::{
        fetch_and_preprocess_subgraph_info, fetch_update, Indexing, IndexingId, InternalState,
        NetworkTopologySnapshot, PreprocessedNetworkInfo,
    },
    subgraph_client::Client as SubgraphClient,
    version_filter::{MinimumVersionRequirements, VersionFilter},
};
use crate::{
    config::{BlockedIndexer, BlockedPoi},
    errors::UnavailableReason,
};

/// Subgraph resolution information returned by the [`NetworkService`].
pub struct ResolvedSubgraphInfo {
    /// Subgraph chain name.
    // This is the chain name is used to retrieve the latest known block number for the chain
    // from the chain head tracking service.
    pub chain: String,
    /// Subgraph start block number.
    pub start_block: BlockNumber,
    /// The [`SubgraphId`]s associated with the query selector.
    pub subgraphs: Vec<SubgraphId>,
    /// Subgraph versions, in descending order.
    pub versions: Vec<DeploymentId>,
    /// A list of [`Indexing`]s for the resolved subgraph versions.
    pub indexings: HashMap<IndexingId, Result<Indexing, UnavailableReason>>,
}

impl ResolvedSubgraphInfo {
    /// Get the latest block number reported.
    ///
    /// The latest block number is the highest block number among all the reported progress of
    /// the indexings associated with the resolved subgraph.
    pub fn latest_reported_block(&self) -> Option<BlockNumber> {
        self.indexings
            .values()
            .filter_map(|indexing| {
                indexing
                    .as_ref()
                    .ok()
                    .map(|indexing| indexing.progress.latest_block)
            })
            .max()
    }
}

/// The [`NetworkService`] is responsible for extracting and providing information about
/// the network topology and subgraphs associated with a given query selector, e.g., a subgraph ID.
///
/// To create a new [`NetworkService`] instance, use the [`NetworkServiceBuilder`].
#[derive(Clone)]
pub struct NetworkService {
    network: watch::Receiver<NetworkTopologySnapshot>,
}

impl NetworkService {
    /// Wait for the network topology information to be available.
    pub async fn wait_until_ready(&mut self) {
        self.network
            .wait_for(|n| !n.subgraphs.is_empty())
            .await
            .unwrap();
    }

    /// Wait for the network topology information to change.
    pub async fn changed(&mut self) {
        self.network.changed().await.unwrap();
    }

    /// Given a [`SubgraphId`], resolve the deployments associated with the subgraph.
    ///
    /// If the subgraph is not found, returns `Ok(None)`.
    pub fn resolve_with_subgraph_id(
        &self,
        id: &SubgraphId,
    ) -> Result<Option<ResolvedSubgraphInfo>, SubgraphError> {
        let network = self.network.borrow();

        // Resolve the subgraph information
        let subgraph = match network.subgraphs.get(id) {
            None => return Ok(None),
            Some(Err(err)) => return Err(err.to_owned()),
            Some(Ok(subgraph)) => subgraph,
        };

        let subgraph_chain = subgraph.chain.clone();
        let subgraph_start_block = subgraph.start_block;

        Ok(Some(ResolvedSubgraphInfo {
            chain: subgraph_chain,
            start_block: subgraph_start_block,
            subgraphs: vec![subgraph.id],
            versions: subgraph.versions.clone(),
            indexings: subgraph.indexings.clone(),
        }))
    }

    /// Given a [`DeploymentId`], resolve the deployments associated with the subgraph.
    ///
    /// If the deployment is not found, returns `Ok(None)`.
    pub fn resolve_with_deployment_id(
        &self,
        id: &DeploymentId,
    ) -> Result<Option<ResolvedSubgraphInfo>, DeploymentError> {
        let network = self.network.borrow();

        // Resolve the deployment information
        let deployment = match network.deployments.get(id) {
            None => return Ok(None),
            Some(Err(err)) => return Err(err.to_owned()),
            Some(Ok(deployment)) => deployment,
        };

        let deployment_chain = deployment.chain.clone();
        let deployment_start_block = deployment.start_block;

        let subgraphs = deployment.subgraphs.iter().copied().collect::<Vec<_>>();
        let indexings = deployment.indexings.clone();

        Ok(Some(ResolvedSubgraphInfo {
            chain: deployment_chain,
            start_block: deployment_start_block,
            subgraphs,
            versions: vec![*id],
            indexings,
        }))
    }

    /// Get the latest indexed block number reported by the indexers.
    pub fn indexing_progress(&self) -> HashMap<IndexingId, BlockNumber> {
        self.network
            .borrow()
            .deployments
            .iter()
            .flat_map(|(_, result)| result.iter().flat_map(|d| &d.indexings))
            .flat_map(|(id, indexing)| indexing.iter().map(|i| (*id, i.progress.latest_block)))
            .collect()
    }
}

pub fn spawn(
    http: reqwest::Client,
    subgraph_client: SubgraphClient,
    min_indexer_service_version: Version,
    min_graph_node_version: Version,
    indexer_blocklist: BTreeMap<Address, BlockedIndexer>,
    indexer_host_blocklist: HashSet<IpNetwork>,
    poi_blocklist: Vec<BlockedPoi>,
) -> NetworkService {
    let internal_state = InternalState {
        indexer_blocklist,
        indexer_host_filter: HostFilter::new(indexer_host_blocklist)
            .expect("failed to create host resolver"),
        indexer_version_filter: VersionFilter::new(
            http.clone(),
            MinimumVersionRequirements {
                indexer_service: min_indexer_service_version,
                graph_node: min_graph_node_version,
            },
        ),
        poi_blocklist: PoiBlocklist::new(poi_blocklist),
        poi_resolver: PoiResolver::new(
            http.clone(),
            Duration::from_secs(5),
            Duration::from_secs(20 * 60),
        ),
        indexing_progress_resolver: IndexingProgressResolver::new(
            http.clone(),
            Duration::from_secs(25),
        ),
        cost_model_resolver: CostModelResolver::new(http.clone()),
    };
    let update_interval = Duration::from_secs(60);
    let network = spawn_updater_task(subgraph_client, internal_state, update_interval);

    NetworkService { network }
}

/// Spawn a background task to fetch the network topology information from the graph network
/// subgraph at regular intervals
fn spawn_updater_task(
    mut subgraph_client: SubgraphClient,
    state: InternalState,
    update_interval: Duration,
) -> watch::Receiver<NetworkTopologySnapshot> {
    let (tx, rx) = watch::channel(Default::default());

    tokio::spawn(async move {
        let mut network_info: Option<PreprocessedNetworkInfo> = None;

        let mut timer = tokio::time::interval(update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            timer.tick().await;

            match fetch_and_preprocess_subgraph_info(&mut subgraph_client, update_interval).await {
                Ok(info) => network_info = Some(info),
                Err(network_subgraph_update_err) => tracing::error!(%network_subgraph_update_err),
            };
            let network_info = match &network_info {
                Some(info) => info,
                None => continue,
            };
            let snapshot = fetch_update(network_info, &state).await;
            tracing::info!(
                subgraphs = snapshot.subgraphs.len(),
                deployments = snapshot.deployments.len(),
                indexings = snapshot
                    .deployments
                    .values()
                    .filter_map(|d| d.as_ref().ok())
                    .map(|d| d.indexings.len())
                    .sum::<usize>(),
            );

            let _ = tx.send(snapshot);
        }
    });

    rx
}
