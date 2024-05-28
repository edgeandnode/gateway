//! The [`NetworkService`] is a `graph-gateway` specific abstraction layer providing a
//! simplified interface for resolving the subgraph-specific information required by the
//! query processing pipeline

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::anyhow;
use eventuals::{Eventual, EventualExt as _, Ptr};
use gateway_framework::errors::Error;
use ipnetwork::IpNetwork;
use semver::Version;
use tokio::sync::Mutex;
use vec1::{vec1, Vec1};

use super::{
    indexer_addr_blocklist::AddrBlocklist,
    indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::HostResolver,
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_poi_blocklist::PoiBlocklist,
    indexer_indexing_poi_resolver::PoiResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::{VersionResolver, DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT},
    internal::fetch_update,
    snapshot::{
        Address, BlockNumber, DeploymentId, Indexing, IndexingError, IndexingId,
        NetworkTopologySnapshot, SubgraphId,
    },
    subgraph::Client as SubgraphClient,
};
use crate::{
    indexers::public_poi::ProofOfIndexingInfo,
    network::{
        indexer_host_resolver::DEFAULT_INDEXER_HOST_RESOLUTION_TIMEOUT,
        indexer_indexing_cost_model_resolver::DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT,
        indexer_indexing_poi_resolver::DEFAULT_INDEXER_INDEXING_POIS_RESOLUTION_TIMEOUT,
        indexer_indexing_progress_resolver::DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT,
        internal::{InternalState, VersionRequirements as IndexerVersionRequirements},
    },
};

/// Default update interval for the network topology information.
pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(30);

pub enum SubgraphResolution {
    /// The subgraph has been transferred to L2.
    TransferredToL2 { id_on_l2: Option<SubgraphId> },
    /// The subgraph not found.
    NotFound,
    /// The subgraph was resolved successfully.
    Resolved(ResolvedSubgraphInfo),
}

/// Subgraph resolution information returned by the [`NetworkService`].
pub struct ResolvedSubgraphInfo {
    /// Subgraph chain name.
    // This is the chain name is used to retrieve the latest known block number for the chain
    // from the chain head tracking service.
    pub chain: String,
    /// Subgraph start block number.
    pub start_block: BlockNumber,

    /// The [`SubgraphId`]s associated with the query selector.
    pub subgraphs: Vec1<SubgraphId>,
    /// The [`DeploymentId`]s associated with the query selector.
    pub deployments: Vec1<DeploymentId>,

    /// A list of [`Indexing`]s for the resolved subgraph versions.
    pub indexings: HashMap<IndexingId, Result<Indexing, IndexingError>>,
}

impl ResolvedSubgraphInfo {
    /// Get the latest indexed block number for the resolved subgraph.
    ///
    /// The latest block number is the highest block number among all the indexings associated with
    /// the resolved subgraph.
    pub fn latest_indexed_block(&self) -> BlockNumber {
        self.indexings
            .values()
            .filter_map(|indexing| indexing.as_ref().ok())
            .map(|indexing| indexing.progress.latest_block)
            .max()
            .unwrap_or(self.start_block)
    }
}

/// The [`NetworkService`] is responsible for extracting and providing information about
/// the network topology and subgraphs associated with a given query selector, e.g., a subgraph ID.
///
/// To create a new [`NetworkService`] instance, use the [`NetworkServiceBuilder`].
#[derive(Clone)]
pub struct NetworkService {
    network: Eventual<Ptr<NetworkTopologySnapshot>>,
}

impl NetworkService {
    /// Wait for the network topology information to be available.
    pub async fn wait_until_ready(&self) {
        let _ = self
            .network
            .value()
            .await
            .expect("network service not available");
    }

    /// Get the deployments table as an eventual.
    // TODO: For backwards-compat. Review this method and consider removing it
    //   - This method is used in the `main.rs` file to construct a map of indexings to
    //     their largest allocation address. This is consumed by the `scalar::ReceiptSigner`.
    //   - This is consumed by the indexing performance service/actor.
    pub fn indexings(&self) -> Eventual<Ptr<HashMap<IndexingId, Indexing>>> {
        self.network.clone().map(|network| async move {
            let indexings = network
                .subgraphs()
                .values()
                .flat_map(|subgraph| subgraph.indexings.clone())
                .filter_map(|(id, indexing)| indexing.map(|indexing| (id, indexing)).ok())
                .collect();

            Ptr::new(indexings)
        })
    }

    /// Given a [`SubgraphId`], resolve the deployments associated with the subgraph.
    ///
    /// If the subgraph is not found, returns `Ok(None)`.
    pub fn resolve_with_subgraph_id(&self, id: &SubgraphId) -> anyhow::Result<SubgraphResolution> {
        let network = self
            .network
            .value_immediate()
            .ok_or(Error::Internal(anyhow!("network topology not available")))?;

        // Check if the subgraph is transferred to L2
        if let Some(id_on_l2) = network.transferred_subgraphs().get(id) {
            return Ok(SubgraphResolution::TransferredToL2 {
                id_on_l2: Some(*id_on_l2),
            });
        }

        // Resolve the subgraph information
        let subgraph = match network.get_subgraph_by_id(id) {
            Some(subgraph) => subgraph,
            None => return Ok(SubgraphResolution::NotFound),
        };

        let subgraph_chain = subgraph.chain.clone();
        let subgraph_start_block = subgraph.start_block;

        let subgraphs = vec1![subgraph.id];
        let deployments = subgraph
            .deployments
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| anyhow!("no deployments found for subgraph {id}"))?;

        let indexings = subgraph.indexings.clone();

        Ok(SubgraphResolution::Resolved(ResolvedSubgraphInfo {
            chain: subgraph_chain,
            start_block: subgraph_start_block,
            subgraphs,
            deployments,
            indexings,
        }))
    }

    /// Given a [`DeploymentId`], resolve the deployments associated with the subgraph.
    ///
    /// If the deployment is not found, returns `Ok(None)`.
    pub fn resolve_with_deployment_id(
        &self,
        id: &DeploymentId,
    ) -> anyhow::Result<SubgraphResolution> {
        let network = self
            .network
            .value_immediate()
            .ok_or(Error::Internal(anyhow!("network topology not available")))?;

        // Check if the deployment is transferred to L2
        if network.transferred_deployments().contains(id) {
            return Ok(SubgraphResolution::TransferredToL2 { id_on_l2: None });
        }

        // Resolve the deployment information
        let deployment = match network.get_deployment_by_id(id) {
            Some(deployment) => deployment,
            None => return Ok(SubgraphResolution::NotFound),
        };

        let deployment_chain = deployment.chain.clone();
        let deployment_start_block = deployment.start_block;

        let subgraphs = deployment
            .subgraphs
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| anyhow!("no subgraphs found for deployment {id}"))?;
        let deployments = vec1![deployment.id];

        let indexings = deployment.indexings.clone();

        Ok(SubgraphResolution::Resolved(ResolvedSubgraphInfo {
            chain: deployment_chain,
            start_block: deployment_start_block,
            subgraphs,
            deployments,
            indexings,
        }))
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
    indexer_indexing_pois_blocklist: Option<(PoiBlocklist, PoiResolver)>,
    indexer_indexing_progress_resolver: IndexingProgressResolver,
    indexer_indexing_cost_model_resolver: CostModelResolver,
    indexer_indexing_cost_model_compiler: CostModelCompiler,
    update_interval: Duration,
}

impl NetworkServiceBuilder {
    /// Creates a new [`NetworkServiceBuilder`] instance.
    pub fn new(subgraph_client: SubgraphClient, indexer_client: reqwest::Client) -> Self {
        let indexer_host_resolver = HostResolver::with_timeout(
            DEFAULT_INDEXER_HOST_RESOLUTION_TIMEOUT, // 1500ms
        )
        .expect("failed to create host resolver");
        let indexer_version_resolver = VersionResolver::with_timeout(
            indexer_client.clone(),
            DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT, // 1500ms
        );
        let indexer_indexing_progress_resolver = IndexingProgressResolver::with_timeout(
            indexer_client.clone(),
            DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT, // 5s
        );
        let indexer_indexing_cost_model_resolver = CostModelResolver::with_timeout(
            indexer_client.clone(),
            DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT, // 5s
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

    /// Sets the minimum agent version for indexers.
    pub fn with_indexer_min_agent_version(mut self, version: Version) -> Self {
        self.indexer_version_requirements.min_agent_version = version;
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
    pub fn with_indexer_pois_blocklist(mut self, blocklist: HashSet<ProofOfIndexingInfo>) -> Self {
        let resolver = PoiResolver::with_timeout(
            self.indexer_client.clone(),
            DEFAULT_INDEXER_INDEXING_POIS_RESOLUTION_TIMEOUT, // 5s
        );
        let blocklist = PoiBlocklist::new(blocklist);

        self.indexer_indexing_pois_blocklist = Some((blocklist, resolver));
        self
    }

    /// Builds the [`NetworkService`] instance ready for spawning.
    ///
    /// To spawn the [`NetworkService`] instance, call the [`NetworkServicePending::spawn`] method.
    pub fn build(self) -> NetworkServicePending {
        let internal_state = InternalState {
            indexer_addr_blocklist: self.indexer_addr_blocklist,
            indexer_host_resolver: Mutex::new(self.indexer_host_resolver),
            indexer_host_blocklist: self.indexer_host_blocklist,
            indexer_version_requirements: self.indexer_version_requirements,
            indexer_version_resolver: self.indexer_version_resolver,
            indexer_indexing_pois_blocklist: self
                .indexer_indexing_pois_blocklist
                .map(|(bl, res)| (bl, Mutex::new(res))),
            indexer_indexing_progress_resolver: self.indexer_indexing_progress_resolver,
            indexer_indexing_cost_model_resolver: (
                self.indexer_indexing_cost_model_resolver,
                Mutex::new(self.indexer_indexing_cost_model_compiler),
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
) -> Eventual<Ptr<NetworkTopologySnapshot>> {
    let (mut eventual_writer, eventual) = Eventual::new();

    tokio::spawn(async move {
        loop {
            // Fetch the network topology information every `update_interval` duration
            // If the fetch fails or takes too long, log a warning and skip the update
            tokio::select! { biased;
                update = fetch_update(&subgraph_client, &state) => {
                    match update {
                        Ok(network) => {
                            eventual_writer.write(Ptr::new(network));
                        }
                        // If the fetch fails, log a warning and skip the update
                        Err(err) => {
                            tracing::warn!(network_update_err=%err);
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

    eventual
}
