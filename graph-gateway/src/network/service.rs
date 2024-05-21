//! The [`NetworkService`] is a `graph-gateway` specific abstraction layer providing a
//! simplified interface for resolving the subgraph-specific information required by the
//! query processing pipeline

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::anyhow;
use eventuals::{Eventual, EventualExt, Ptr};
use gateway_framework::errors::Error;
use ipnetwork::IpNetwork;
use semver::Version;
use tokio::sync::Mutex;
use vec1::{vec1, Vec1};

use super::{
    indexers_addr_blocklist::AddrBlocklist,
    indexers_cost_model_compiler::CostModelCompiler,
    indexers_cost_model_resolver::CostModelResolver,
    indexers_host_blocklist::HostBlocklist,
    indexers_host_resolver::HostResolver,
    indexers_indexing_status_resolver::IndexingStatusResolver,
    indexers_poi_blocklist::PoiBlocklist,
    indexers_poi_resolver::PoiResolver,
    internal::{fetch_update, InternalState},
    snapshot::{
        Address, BlockNumber, DeploymentId, Indexing, IndexingId, NetworkTopologySnapshot,
        SubgraphId,
    },
    subgraph::Client as SubgraphClient,
};
use crate::indexers::public_poi::ProofOfIndexingInfo;

/// Default update interval for the network topology information.
pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(30);

/// Subgraph resolution information returned by the [`NetworkService`].
pub struct ResolvedSubgraphInfo {
    /// Subgraph chain name.
    // This is the chain name is used to retrieve the latest known block number for the chain
    // from the chain head tracking service.
    pub chain: String,
    /// Subgraph start block number.
    pub start_block: BlockNumber,

    /// The [`SubgraphId`] on L2.
    // NOTE: Required only for L1-to-L2 gateway query redirection
    pub id_on_l2: Option<SubgraphId>,
    /// Whether the subgraph has been transferred to L2.
    // NOTE: Required only for L1-to-L2 gateway query redirection
    pub transferred_to_l2: bool,

    /// The [`SubgraphId`]s associated with the query selector.
    pub subgraphs: Vec1<SubgraphId>,
    /// The [`DeploymentId`]s associated with the query selector.
    pub deployments: Vec1<DeploymentId>,

    /// A list of [`Indexing`]s for the resolved subgraph versions.
    pub indexings: HashMap<IndexingId, Indexing>,
}

impl ResolvedSubgraphInfo {
    /// Get the latest indexed block number for the resolved subgraph.
    ///
    /// The latest block number is the highest block number among all the indexings associated with
    /// the resolved subgraph.
    pub fn latest_indexed_block(&self) -> BlockNumber {
        self.indexings
            .values()
            .filter_map(|indexing| indexing.status.as_ref())
            .map(|status| status.latest_block)
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
                .subgraphs
                .values()
                .flat_map(|subgraph| subgraph.indexings.clone())
                .collect();

            Ptr::new(indexings)
        })
    }

    /// Given a [`SubgraphId`], resolve the deployments associated with the subgraph.
    ///
    /// If the subgraph is not found, returns `Ok(None)`.
    pub fn resolve_with_subgraph_id(
        &self,
        id: &SubgraphId,
    ) -> anyhow::Result<Option<ResolvedSubgraphInfo>> {
        let subgraph = match self
            .network
            .value_immediate()
            .ok_or(anyhow!("network topology not available"))?
            .get_subgraph_by_id(id)
        {
            Some(subgraph) => subgraph,
            None => return Ok(None),
        };

        let subgraph_chain = subgraph.chain.clone();
        let subgraph_start_block = subgraph.start_block;
        let subgraph_id_on_l2 = subgraph.l2_id;
        let subgraph_transferred_to_l2 = subgraph.transferred_to_l2;

        let subgraphs = vec1![subgraph.id];
        let deployments = subgraph
            .deployments
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| anyhow!("no deployments found for subgraph {id}"))?;

        let indexings = subgraph.indexings.clone();

        Ok(Some(ResolvedSubgraphInfo {
            chain: subgraph_chain,
            start_block: subgraph_start_block,
            id_on_l2: subgraph_id_on_l2,
            transferred_to_l2: subgraph_transferred_to_l2,
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
    ) -> anyhow::Result<Option<ResolvedSubgraphInfo>> {
        let deployment = match self
            .network
            .value_immediate()
            .ok_or(Error::Internal(anyhow!("network topology not available")))?
            .get_deployment_by_id(id)
        {
            Some(deployment) => deployment,
            None => return Ok(None),
        };

        let deployment_chain = deployment.chain.clone();
        let deployment_start_block = deployment.start_block;
        let deployment_transferred_to_l2 = deployment.transferred_to_l2;

        let subgraphs = deployment
            .subgraphs
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| anyhow!("no subgraphs found for deployment {id}"))?;
        let deployments = vec1![deployment.id];

        let indexings = deployment.indexings.clone();

        Ok(Some(ResolvedSubgraphInfo {
            chain: deployment_chain,
            start_block: deployment_start_block,
            id_on_l2: None,
            transferred_to_l2: deployment_transferred_to_l2,
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
    indexer_min_agent_version: Version,
    indexer_min_scalar_tap_version: Version,
    indexer_min_graph_node_version: Version,
    indexer_addr_blocklist: Option<AddrBlocklist>,
    indexer_host_resolver: HostResolver,
    indexer_host_blocklist: Option<HostBlocklist>,
    indexer_pois_blocklist: Option<(PoiBlocklist, PoiResolver)>,
    indexer_indexing_status_resolver: IndexingStatusResolver,
    indexer_cost_model_resolver: CostModelResolver,
    indexer_cost_model_compiler: CostModelCompiler,
    update_interval: Duration,
}

impl NetworkServiceBuilder {
    /// Creates a new [`NetworkServiceBuilder`] instance.
    pub fn new(subgraph_client: SubgraphClient, indexer_client: reqwest::Client) -> Self {
        let indexer_host_resolver = HostResolver::new().expect("failed to create host resolver");
        let indexer_indexing_status_resolver = IndexingStatusResolver::new(indexer_client.clone());
        let indexer_cost_model_resolver = CostModelResolver::new(indexer_client.clone());
        let indexer_cost_model_compiler = CostModelCompiler::default();

        Self {
            subgraph_client,
            indexer_client,
            indexer_min_agent_version: Version::new(0, 0, 0),
            indexer_min_scalar_tap_version: Version::new(0, 0, 0),
            indexer_min_graph_node_version: Version::new(0, 0, 0),
            indexer_addr_blocklist: None,
            indexer_host_resolver,
            indexer_host_blocklist: None,
            indexer_pois_blocklist: None,
            indexer_indexing_status_resolver,
            indexer_cost_model_resolver,
            indexer_cost_model_compiler,
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
        self.indexer_min_agent_version = version;
        self
    }

    /// Sets the minimum scalar tap version for indexers.
    pub fn with_indexer_min_scalar_tap_version(mut self, version: Version) -> Self {
        self.indexer_min_scalar_tap_version = version;
        self
    }

    /// Sets the minimum graph node version for indexers.
    pub fn with_indexer_min_graph_node_version(mut self, version: Version) -> Self {
        self.indexer_min_graph_node_version = version;
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
        let resolver = PoiResolver::new(self.indexer_client.clone());
        let blocklist = PoiBlocklist::new(blocklist);

        self.indexer_pois_blocklist = Some((blocklist, resolver));
        self
    }

    /// Builds the [`NetworkService`] instance ready for spawning.
    ///
    /// To spawn the [`NetworkService`] instance, call the [`NetworkServicePending::spawn`] method.
    pub fn build(self) -> NetworkServicePending {
        let internal_state = InternalState {
            indexers_http_client: self.indexer_client,
            indexers_min_agent_version: self.indexer_min_agent_version,
            indexers_min_scalar_tap_version: self.indexer_min_scalar_tap_version,
            indexers_min_graph_node_version: self.indexer_min_graph_node_version,
            indexers_addr_blocklist: self.indexer_addr_blocklist,
            indexers_host_resolver: Mutex::new(self.indexer_host_resolver),
            indexers_host_blocklist: self.indexer_host_blocklist,
            indexers_pois_blocklist: self
                .indexer_pois_blocklist
                .map(|(bl, res)| (bl, Mutex::new(res))),
            indexers_indexing_status_resolver: self.indexer_indexing_status_resolver,
            indexers_cost_model_resolver: (
                self.indexer_cost_model_resolver,
                Mutex::new(self.indexer_cost_model_compiler),
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
        let subgraph_client = Mutex::new(subgraph_client);
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
