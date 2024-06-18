//! The [`NetworkService`] is a `graph-gateway` specific abstraction layer providing a
//! simplified interface for resolving the subgraph-specific information required by the
//! query processing pipeline

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloy_primitives::{Address, BlockNumber};
use gateway_common::ttl_hash_map::DEFAULT_TTL;
use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::types::{DeploymentId, ProofOfIndexing, SubgraphId};
use tokio::{sync::watch, time::MissedTickBehavior};

use super::{
    config::VersionRequirements as IndexerVersionRequirements,
    errors::{DeploymentError, SubgraphError},
    indexer_addr_blocklist::AddrBlocklist,
    indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::{HostResolver, DEFAULT_INDEXER_HOST_RESOLUTION_TIMEOUT},
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::{
        CostModelResolver, DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT,
    },
    indexer_indexing_poi_blocklist::PoiBlocklist,
    indexer_indexing_poi_resolver::{
        PoiResolver, DEFAULT_INDEXER_INDEXING_POIS_RESOLUTION_TIMEOUT,
    },
    indexer_indexing_progress_resolver::{
        IndexingProgressResolver, DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT,
    },
    indexer_version_resolver::{VersionResolver, DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT},
    internal::{fetch_update, Indexing, IndexingId, InternalState, NetworkTopologySnapshot},
    subgraph_client::Client as SubgraphClient,
    ResolutionError,
};

/// Default update interval for the network topology information.
pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(60);

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

    /// A list of [`Indexing`]s for the resolved subgraph versions.
    pub indexings: HashMap<IndexingId, Result<Indexing, ResolutionError>>,
}

impl ResolvedSubgraphInfo {
    /// Get the latest block number reported.
    ///
    /// The latest block number is the highest block number among all the reported progress of
    /// the indexings associated with the resolved subgraph. Ignore errored or stale indexings'
    /// progress information.
    pub fn latest_reported_block(&self) -> Option<BlockNumber> {
        self.indexings
            .values()
            .filter_map(|indexing| indexing.as_ref().ok())
            .filter_map(|indexing| indexing.progress.as_fresh())
            .map(|progress| progress.latest_block)
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
            .wait_for(|n| !n.subgraphs().is_empty())
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
        let subgraph = match network.get_subgraph_by_id(id) {
            None => return Ok(None),
            Some(Err(err)) => return Err(err.to_owned()),
            Some(Ok(subgraph)) => subgraph,
        };

        let subgraph_chain = subgraph.chain.clone();
        let subgraph_start_block = subgraph.start_block;

        let subgraphs = vec![subgraph.id];
        let indexings = subgraph
            .indexings
            .clone()
            .into_iter()
            .map(|(id, res)| (id, res.map_err(|err| err.into())))
            .collect();

        Ok(Some(ResolvedSubgraphInfo {
            chain: subgraph_chain,
            start_block: subgraph_start_block,
            subgraphs,
            indexings,
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
        let deployment = match network.get_deployment_by_id(id) {
            None => return Ok(None),
            Some(Err(err)) => return Err(err.to_owned()),
            Some(Ok(deployment)) => deployment,
        };

        let deployment_chain = deployment.chain.clone();
        let deployment_start_block = deployment.start_block;

        let subgraphs = deployment.subgraphs.iter().copied().collect::<Vec<_>>();
        let indexings = deployment
            .indexings
            .clone()
            .into_iter()
            .map(|(id, res)| (id, res.map_err(|err| err.into())))
            .collect();

        Ok(Some(ResolvedSubgraphInfo {
            chain: deployment_chain,
            start_block: deployment_start_block,
            subgraphs,
            indexings,
        }))
    }

    /// Get the latest indexed block number reported by the indexers.
    pub fn indexing_progress(&self) -> HashMap<IndexingId, BlockNumber> {
        self.network
            .borrow()
            .deployments()
            .iter()
            .flat_map(|(_, result)| result.iter().flat_map(|d| &d.indexings))
            .flat_map(|(id, indexing)| indexing.iter().map(|i| (*id, i.progress.latest_block)))
            .collect()
    }
}

/// The [`NetworkService`] builder.
pub struct NetworkServiceBuilder {
    subgraph_client: SubgraphClient,
    indexer_client: reqwest::Client,
    indexer_addr_blocklist: Option<AddrBlocklist>,
    indexer_host_resolver: HostResolver,
    indexer_host_blocklist: Option<HostBlocklist>,
    indexer_version_requirements: IndexerVersionRequirements,
    indexer_version_resolver: VersionResolver,
    indexer_indexing_pois_blocklist: Option<(PoiResolver, PoiBlocklist)>,
    indexer_indexing_progress_resolver: IndexingProgressResolver,
    indexer_indexing_cost_model_resolver: CostModelResolver,
    indexer_indexing_cost_model_compiler: CostModelCompiler,
    update_interval: Duration,
}

impl NetworkServiceBuilder {
    /// Creates a new [`NetworkServiceBuilder`] instance.
    pub fn new(subgraph_client: SubgraphClient, indexer_client: reqwest::Client) -> Self {
        let indexer_host_resolver = HostResolver::with_timeout(
            DEFAULT_INDEXER_HOST_RESOLUTION_TIMEOUT, // 5 seconds
        )
        .expect("failed to create host resolver");
        let indexer_version_resolver = VersionResolver::with_timeout_and_cache_ttl(
            indexer_client.clone(),
            DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT, // 5 seconds
            DEFAULT_TTL,                                // Duration::MAX
        );
        let indexer_indexing_progress_resolver =
            IndexingProgressResolver::with_timeout_and_cache_ttl(
                indexer_client.clone(),
                DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT, // 25 seconds
                DEFAULT_TTL,                                          // Duration::MAX
            );
        let indexer_indexing_cost_model_resolver = CostModelResolver::with_timeout_and_cache_ttl(
            indexer_client.clone(),
            DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT, // 5 seconds
            DEFAULT_TTL,                                            // Duration::MAX
        );
        let indexer_indexing_cost_model_compiler = CostModelCompiler::default();

        Self {
            subgraph_client,
            indexer_client,
            indexer_addr_blocklist: None,
            indexer_host_resolver,
            indexer_host_blocklist: None,
            indexer_version_requirements: Default::default(),
            indexer_version_resolver,
            indexer_indexing_pois_blocklist: None,
            indexer_indexing_progress_resolver,
            indexer_indexing_cost_model_resolver,
            indexer_indexing_cost_model_compiler,
            update_interval: DEFAULT_UPDATE_INTERVAL,
        }
    }

    /// Sets the update interval for the network topology information.
    pub fn with_update_interval(mut self, update_interval: Duration) -> Self {
        self.update_interval = update_interval;
        self
    }

    /// Sets the minimum indexer service version for indexers.
    pub fn with_indexer_min_indexer_service_version(mut self, version: Version) -> Self {
        self.indexer_version_requirements
            .min_indexer_service_version = version;
        self
    }

    /// Sets the minimum graph node version for indexers.
    pub fn with_indexer_min_graph_node_version(mut self, version: Version) -> Self {
        self.indexer_version_requirements.min_graph_node_version = version;
        self
    }

    /// Sets the indexer address blocklist.
    pub fn with_indexer_addr_blocklist(mut self, blocklist: HashSet<Address>) -> Self {
        let blocklist = AddrBlocklist::new(blocklist);

        self.indexer_addr_blocklist = Some(blocklist);
        self
    }

    /// Sets the indexer host blocklist.
    pub fn with_indexer_host_blocklist(mut self, blocklist: HashSet<IpNetwork>) -> Self {
        let blocklist = HostBlocklist::new(blocklist);

        self.indexer_host_blocklist = Some(blocklist);
        self
    }

    /// Sets the indexer POIs blocklist.
    pub fn with_indexer_pois_blocklist(
        mut self,
        blocklist: HashSet<((DeploymentId, BlockNumber), ProofOfIndexing)>,
    ) -> Self {
        let resolver = PoiResolver::with_timeout_and_cache_ttl(
            self.indexer_client.clone(),
            DEFAULT_INDEXER_INDEXING_POIS_RESOLUTION_TIMEOUT, // 5s
            DEFAULT_TTL,                                      // Duration::MAX
        );
        let blocklist = PoiBlocklist::new(blocklist);

        self.indexer_indexing_pois_blocklist = Some((resolver, blocklist));
        self
    }

    /// Builds the [`NetworkService`] instance ready for spawning.
    ///
    /// To spawn the [`NetworkService`] instance, call the [`NetworkServicePending::spawn`] method.
    pub fn build(self) -> NetworkServicePending {
        let internal_state = InternalState {
            indexer_addr_blocklist: self.indexer_addr_blocklist,
            indexer_host_resolver: self.indexer_host_resolver,
            indexer_host_blocklist: self.indexer_host_blocklist,
            indexer_version_requirements: self.indexer_version_requirements,
            indexer_version_resolver: self.indexer_version_resolver,
            indexer_indexing_pois_blocklist: self.indexer_indexing_pois_blocklist,
            indexer_indexing_progress_resolver: self.indexer_indexing_progress_resolver,
            indexer_indexing_cost_model_resolver: (
                self.indexer_indexing_cost_model_resolver,
                self.indexer_indexing_cost_model_compiler,
            ),
        };

        NetworkServicePending {
            subgraph_client: self.subgraph_client,
            internal_state,
            update_interval: self.update_interval,
        }
    }
}

/// The [`NetworkService`] pending instance.
///
/// This struct represents the [`NetworkService`] instance that is pending spawning. To spawn the
/// [`NetworkService`] instance, call the [`NetworkServicePending::spawn`] method.
pub struct NetworkServicePending {
    update_interval: Duration,
    subgraph_client: SubgraphClient,
    internal_state: InternalState,
}

impl NetworkServicePending {
    /// Spawns the [`NetworkService`] instance's background task and returns the service
    /// instance.
    pub fn spawn(self) -> NetworkService {
        let network = spawn_updater_task(
            self.subgraph_client,
            self.internal_state,
            self.update_interval,
        );

        NetworkService { network }
    }
}

/// Spawn a background task to fetch the network topology information from the graph network
/// subgraph at regular intervals
fn spawn_updater_task(
    subgraph_client: SubgraphClient,
    state: InternalState,
    update_interval: Duration,
) -> watch::Receiver<NetworkTopologySnapshot> {
    let (tx, rx) = watch::channel(Default::default());

    tokio::spawn(async move {
        let mut timer = tokio::time::interval(update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Fetch the network topology information every `update_interval` duration
        // If the fetch fails or takes too long, log a warning and skip the update
        loop {
            timer.tick().await;

            tokio::select! { biased;
                update = fetch_update(&subgraph_client, &state) => {
                    match update {
                        Ok(network) => {
                            tracing::info!(
                                subgraphs = network.subgraphs().len(),
                                deployments = network.deployments().len(),
                                indexings = network.deployments()
                                    .values()
                                    .filter_map(|d| d.as_ref().ok())
                                    .map(|d| d.indexings.len())
                                    .sum::<usize>(),
                            );

                            let _ = tx.send(network);
                        }
                        // If the fetch fails, log a warning and skip the update
                        Err(network_update_err) => {
                            tracing::warn!(%network_update_err);
                        }
                    }
                }
                _ = tokio::time::sleep(update_interval) => {
                    // Skip the update if the fetch is taking too long
                    tracing::warn!("network update fetch taking too long");
                }
            }
        }
    });

    rx
}
