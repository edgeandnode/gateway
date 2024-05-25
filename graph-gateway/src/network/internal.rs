use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    time::Duration,
};

use alloy_primitives::Address;
use anyhow::anyhow;
use cost_model::CostModel;
use eventuals::Ptr;
use gateway_common::blocklist::Blocklist as _;
use itertools::Itertools;
use semver::Version;
use thegraph_core::types::{DeploymentId, SubgraphId};
use tokio::sync::Mutex;
use tracing::Instrument;
use url::Url;

use self::types::{
    AllocationInfo, DeploymentInfo, IndexerIndexingInfo, IndexerIndexingRawInfo, IndexerInfo,
    IndexerRawInfo, IndexingProgressInfo, SubgraphInfo, SubgraphVersionInfo,
};
use super::{
    indexer_addr_blocklist::AddrBlocklist,
    indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::{HostResolver, ResolutionError as HostResolutionError},
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_poi_blocklist::PoiBlocklist,
    indexer_indexing_poi_resolver::{PoiResolver, ResolutionError as PoiResolutionError},
    indexer_indexing_progress_resolver::{
        IndexingProgressResolver, ResolutionError as IndexingProgressResolutionError,
    },
    indexer_version_resolver::VersionResolver,
    snapshot,
    snapshot::NetworkTopologySnapshot,
    subgraph,
    subgraph::Client as SubgraphClient,
};

/// The network topology fetch timeout.
///
/// This timeout is applied independently to the indexers and subgraphs information fetches.
const NETWORK_TOPOLOGY_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

/// Internal types.
pub mod types {
    use std::collections::HashMap;

    use alloy_primitives::{Address, BlockNumber};
    use cost_model::CostModel;
    use custom_debug::CustomDebug;
    use eventuals::Ptr;
    use semver::Version;
    use thegraph_core::types::{DeploymentId, SubgraphId};
    use url::Url;

    use crate::network::internal::IndexerIndexingError;

    /// Internal representation of the fetched subgraph information.
    ///
    /// This is not the final representation of the subgraph.
    #[derive(Debug)]
    pub struct SubgraphInfo {
        pub id: SubgraphId,
        pub id_on_l2: Option<SubgraphId>,
        pub versions: Vec<SubgraphVersionInfo>,
    }

    #[derive(Debug)]
    pub struct SubgraphVersionInfo {
        pub version: u32,
        pub deployment: DeploymentInfo,
    }

    /// Internal representation of the fetched deployment information.
    ///
    /// This is not the final representation of the deployment.
    #[derive(Clone, Debug)]
    pub struct DeploymentInfo {
        pub id: DeploymentId,
        pub allocations: Vec<AllocationInfo>,
        pub manifest_network: Option<String>,
        pub manifest_start_block: Option<BlockNumber>,
        pub transferred_to_l2: bool,
    }

    /// Internal representation of the fetched allocation information.
    ///
    /// This is not the final representation of the allocation.
    #[derive(Clone, Debug)]
    pub struct AllocationInfo {
        // The allocation ID.
        pub id: Address,
        // The indexer ID.
        pub indexer: Address,
    }

    /// Internal representation of the indexer pre-processed information.
    ///
    /// This is not the final representation of the indexer.
    #[derive(Clone, CustomDebug)]
    pub struct IndexerRawInfo {
        /// The indexer's ID.
        pub id: Address,
        /// The indexer's URL.
        ///
        /// It is guaranteed that the URL scheme is either HTTP or HTTPS and the URL has a host.
        #[debug(with = std::fmt::Display::fmt)]
        pub url: Url,
        /// The total amount of tokens staked by the indexer.
        pub staked_tokens: u128,
        /// The list of deployments the indexer is associated with.
        ///
        /// The deployments are ordered from highest to lowest associated token allocation.
        //  See ref: d260724b-a445-4842-964e-fb95062c119d
        pub deployments: Vec<DeploymentId>,

        /// The indexer's indexings information.
        pub indexings: HashMap<DeploymentId, IndexerIndexingRawInfo>,
    }

    /// Internal representation of the indexer's indexing information.
    ///
    /// This is not the final representation of the indexer's indexing information.
    #[derive(Clone, Debug)]
    pub struct IndexerIndexingRawInfo {
        /// The largest allocation.
        pub largest_allocation: Address,
        /// The total amount of tokens allocated.
        pub total_allocated_tokens: u128,
    }

    /// Internal representation of the fetched indexer information.
    ///
    /// This is not the final representation of the indexer.
    #[derive(Clone, CustomDebug)]
    pub struct IndexerInfo {
        /// The indexer's ID.
        pub id: Address,
        /// The indexer's URL.
        ///
        /// It is guaranteed that the URL scheme is either HTTP or HTTPS and the URL has a host.
        #[debug(with = std::fmt::Display::fmt)]
        pub url: Url,
        /// The total amount of tokens staked by the indexer.
        pub staked_tokens: u128,
        /// The list of deployments the indexer is associated with.
        ///
        /// The deployments are ordered from highest to lowest associated token allocation.
        //  See ref: d260724b-a445-4842-964e-fb95062c119d
        pub deployments: Vec<DeploymentId>,

        /// The indexer's "indexer service" version.
        pub indexer_agent_version: Version,
        /// The indexer's "graph node" version.
        pub graph_node_version: Version,

        /// The indexer's indexings information.
        pub indexings: HashMap<DeploymentId, Result<IndexerIndexingInfo, IndexerIndexingError>>,
    }

    /// Internal representation of the fetched indexer's indexing information.
    #[derive(Clone, Debug)]
    pub struct IndexerIndexingInfo {
        /// The largest allocation.
        pub largest_allocation: Address,

        /// The total amount of tokens allocated.
        pub total_allocated_tokens: u128,

        /// The indexing progress information
        ///
        /// See [`IndexingProgressInfo`] for more information.
        pub progress: IndexingProgressInfo,

        /// The cost model for this indexing.
        pub cost_model: Option<Ptr<CostModel>>,
    }

    /// Internal representation of the indexing's progress information.
    #[derive(Clone, Debug)]
    pub struct IndexingProgressInfo {
        /// The latest indexed block.
        pub latest_block: BlockNumber,
        /// The minimum indexed block.
        pub min_block: Option<BlockNumber>,
    }
}

/// Internal type holding the network service state.
pub struct InternalState {
    pub indexer_min_agent_version: Version,
    pub indexer_min_graph_node_version: Version,
    pub indexer_addr_blocklist: Option<AddrBlocklist>,
    pub indexer_host_resolver: Mutex<HostResolver>,
    pub indexer_host_blocklist: Option<HostBlocklist>,
    pub indexer_version_resolver: VersionResolver,
    pub indexer_indexing_pois_blocklist: Option<(PoiBlocklist, Mutex<PoiResolver>)>,
    pub indexer_indexing_progress_resolver: IndexingProgressResolver,
    pub indexer_indexing_cost_model_resolver: (CostModelResolver, Mutex<CostModelCompiler>),
}

/// Fetch the network topology information from the graph network subgraph.
pub async fn fetch_update(
    client: &SubgraphClient,
    state: &InternalState,
) -> anyhow::Result<NetworkTopologySnapshot> {
    // Fetch and process the network topology information
    let (indexers_info, subgraphs_info) = futures::future::try_join(
        async {
            let indexers = {
                match tokio::time::timeout(
                    NETWORK_TOPOLOGY_FETCH_TIMEOUT,
                    fetch_and_pre_process_indexers_info(client),
                )
                .await
                {
                    // If the fetch timed out, return an error
                    Err(_) => Err(anyhow!("indexers info fetch timed out")),
                    Ok(resp) => match resp {
                        // If the fetch failed, return an error
                        Err(err) => Err(anyhow!("indexers info fetch failed: {err}")),
                        Ok(resp) => Ok(resp),
                    },
                }
            }?;

            // Process the fetched network topology information
            Ok(process_indexers_info(state, indexers).await)
        },
        async {
            match tokio::time::timeout(
                NETWORK_TOPOLOGY_FETCH_TIMEOUT,
                fetch_and_pre_process_subgraphs_info(client),
            )
            .await
            {
                // If the fetch timed out, return an error
                Err(_) => Err(anyhow!("subgraphs info fetch timed out")),
                Ok(resp) => match resp {
                    // If the fetch failed, return an error
                    Err(err) => Err(anyhow!("subgraphs info fetch failed: {err}")),
                    Ok(resp) => Ok(resp),
                },
            }
        },
    )
    .await?;

    Ok(snapshot::new_from(indexers_info, subgraphs_info))
}

/// Fetch the indexers information from the graph network subgraph and performs pre-processing
/// steps, i.e., validation and conversion into the internal representation.
///
///   1. Fetch the indexers information from the graph network subgraph.
///   2. Validate and convert the indexers fetched info into the internal representation.
///
/// If the fetch fails or the response is empty, an error is returned.
///
/// Invalid info is filtered out before converting into the internal representation. If no valid
/// indexers are found, an error is returned.
pub async fn fetch_and_pre_process_indexers_info(
    client: &SubgraphClient,
) -> anyhow::Result<HashMap<Address, IndexerRawInfo>> {
    // Fetch the indexers information from the graph network subgraph
    let indexers = client
        .fetch_indexers()
        .await
        .map_err(|err| anyhow!("indexers fetch failed: {err}"))?;
    if indexers.is_empty() {
        return Err(anyhow!("empty indexers fetch"));
    }

    // Map the fetched indexers info into the internal representation
    // If no valid indexers are found, an error is returned.
    let indexers = indexers
        .into_iter()
        .filter_map(|indexer| {
            let _span =
                tracing::debug_span!("indexer pre-processing", indexer.id = %indexer.id).entered();
            tracing::trace!(
                indexer.allocations_count = %indexer.allocations.len(),
                indexer.url = ?indexer.url,
            );

            match try_into_internal_indexer_info(indexer) {
                Ok(indexer) => Some((indexer.id, indexer)),
                Err(err) => {
                    tracing::debug!("filtering-out indexer: {err}");
                    None
                }
            }
        })
        .collect::<HashMap<_, _>>();

    // If no valid indexers are found, return an error
    if indexers.is_empty() {
        Err(anyhow!("no valid indexers found"))
    } else {
        Ok(indexers)
    }
}

/// Fetch the subgraphs information from the graph network subgraph and performs pre-processing
/// steps, i.e., validation and conversion into the internal representation.
///
///   1. Fetch the subgraphs information from the graph network subgraph.
///   2. Validate and convert the subgraphs fetched info into the internal representation.
///
/// If the fetch fails or the response is empty, an error is returned.
///
/// Invalid info is filtered out before converting into the internal representation. If no valid
/// subgraphs are found, an error is returned.
pub async fn fetch_and_pre_process_subgraphs_info(
    client: &SubgraphClient,
) -> anyhow::Result<HashMap<SubgraphId, SubgraphInfo>> {
    // Fetch the subgraphs information from the graph network subgraph
    let subgraphs = client
        .fetch_subgraphs()
        .await
        .map_err(|err| anyhow!("subgraphs fetch failed: {err}"))?;
    if subgraphs.is_empty() {
        return Err(anyhow!("empty subgraphs fetch"));
    }

    // Map the fetched subgraphs info into the internal representation
    // If no valid subgraphs are found, an error is returned.
    let subgraphs = subgraphs
        .into_iter()
        .filter_map(|subgraph| {
            let _span = tracing::debug_span!(
                "subgraph pre-processing",
                subgraph.id = %subgraph.id,
            )
            .entered();
            match try_into_internal_subgraph_info(subgraph) {
                Ok(subgraph) => Some((subgraph.id, subgraph)),
                Err(err) => {
                    tracing::debug!("filtering-out subgraph: {err}");
                    None
                }
            }
        })
        .collect::<HashMap<_, _>>();

    // If no valid subgraphs are found, return an error
    if subgraphs.is_empty() {
        Err(anyhow!("no valid subgraphs found"))
    } else {
        Ok(subgraphs)
    }
}

/// Convert from the fetched indexer information into the internal representation.
///
/// If the indexer is invalid, e.g., has no URL, an error is returned.
fn try_into_internal_indexer_info(
    indexer: subgraph::types::fetch_indexers::Indexer,
) -> anyhow::Result<IndexerRawInfo> {
    // Check if the indexer is present
    let indexer_url = indexer.url.ok_or_else(|| anyhow!("missing URL"))?;

    // Parse the URL. It must have an HTTP (or HTTPS) scheme and a valid host.
    // Filter out indexers with invalid URLs.
    let indexer_url = indexer_url
        .parse::<Url>()
        .map_err(|err| anyhow!("invalid URL: parsing failed: {err}"))?;
    if !indexer_url.scheme().starts_with("http") {
        return Err(anyhow!("invalid URL: invalid scheme"));
    }
    if indexer_url.host().is_none() {
        return Err(anyhow!("invalid URL: missing host"));
    }

    // Check if the indexer has any allocations
    if indexer.allocations.is_empty() {
        return Err(anyhow!("no allocations"));
    }

    // Get the list of unique deployment IDs the indexer is associated with.
    // NOTE: The indexer is guaranteed to have at least one allocation and one
    // deployment.
    // See ref: d260724b-a445-4842-964e-fb95062c119d
    let indexer_deployment_ids = indexer
        .allocations
        .iter()
        .map(|alloc| alloc.subgraph_deployment.id)
        .unique()
        .collect::<Vec<_>>();
    if indexer_deployment_ids.is_empty() {
        return Err(anyhow!("no deployments"));
    }

    // Create the indexings' information table for the indexer
    let indexer_indexings_info = indexer_deployment_ids
        .iter()
        .filter_map(|deployment_id| {
            // Get the largest allocation and the total amount of tokens allocated for each indexing
            // NOTE: The allocations are ordered by `allocatedTokens` in descending order, and
            // the largest allocation is the first one.
            // See ref: d260724b-a445-4842-964e-fb95062c119d
            let mut indexer_allocations_iter = indexer
                .allocations
                .iter()
                .filter(|alloc| alloc.subgraph_deployment.id == *deployment_id)
                .peekable();

            // To avoid cloning the iterator, as we are interested in the first element, we use a
            // "peekable" iterator to "peek" the next element of the iterator without consuming it.
            let largest_allocation = indexer_allocations_iter.peek().map(|alloc| alloc.id)?;

            // Calculate the total amount of tokens allocated for the deployment
            let total_allocated_tokens = indexer_allocations_iter
                .map(|alloc| alloc.allocated_tokens)
                .sum();

            Some((
                *deployment_id,
                IndexerIndexingRawInfo {
                    largest_allocation,
                    total_allocated_tokens,
                },
            ))
        })
        .collect::<HashMap<_, _>>();

    Ok(IndexerRawInfo {
        id: indexer.id,
        url: indexer_url,
        staked_tokens: indexer.staked_tokens,
        deployments: indexer_deployment_ids,
        indexings: indexer_indexings_info,
    })
}

/// Convert from the fetched subgraph information into the internal representation.
///
/// If the subgraph is invalid, e.g., has no versions, an error is returned.
fn try_into_internal_subgraph_info(
    subgraph: subgraph::types::fetch_subgraphs::Subgraph,
) -> anyhow::Result<SubgraphInfo> {
    let versions = subgraph
        .versions
        .into_iter()
        .map(|version| {
            let deployment = version.subgraph_deployment;

            let deployment_manifest_network = deployment
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.network.clone());
            let deployment_manifest_start_block = deployment
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.start_block);

            let deployment_allocations = deployment
                .allocations
                .into_iter()
                .map(|allocation| AllocationInfo {
                    id: allocation.id,
                    indexer: allocation.indexer.id,
                })
                .collect::<Vec<_>>();

            let deployment_id = deployment.id;
            let deployment_transferred_to_l2 = deployment.transferred_to_l2;

            let version_number = version.version;
            let version_deployment = DeploymentInfo {
                id: deployment_id,
                allocations: deployment_allocations,
                manifest_network: deployment_manifest_network,
                manifest_start_block: deployment_manifest_start_block,
                transferred_to_l2: deployment_transferred_to_l2,
            };

            SubgraphVersionInfo {
                version: version_number,
                deployment: version_deployment,
            }
        })
        .collect::<Vec<_>>();

    if versions.is_empty() {
        return Err(anyhow!("no versions"));
    }

    Ok(SubgraphInfo {
        id: subgraph.id,
        id_on_l2: subgraph.id_on_l2,
        versions,
    })
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexerError {
    /// The indexer was blocked by the address blocklist.
    #[error("indexer address blocked by blocklist")]
    BlockedByAddrBlocklist,

    /// The indexer's host resolution failed.
    #[error("indexer host resolution failed: {0}")]
    HostResolutionFailed(String),
    /// The indexer was blocked by the host blocklist.
    #[error("indexer host blocked by blocklist")]
    BlockedByHostBlocklist,

    /// The indexer's agent version resolution failed.
    #[error("agent version resolution failed: {0}")]
    AgentVersionResolutionFailed(String),
    /// The indexer's agent version is below the minimum required.
    #[error("agent version {0} below the minimum required {1}")]
    AgentVersionBelowMin(Version, Version),

    /// The indexer's graph node version resolution failed.
    #[error("graph-node version resolution failed: {0}")]
    GraphNodeVersionResolutionFailed(String),
    /// The indexer's graph node version is below the minimum required.
    #[error("graph node version {0} below the minimum required {1}")]
    GraphNodeVersionBelowMin(Version, Version),

    /// The indexer's indexing public POIs resolution failed.
    #[error("indexing public POIs resolution failed: {0}")]
    IndexingPoisResolutionFailed(String),
    /// All the indexer's indexings are blocked by the public POIs blocklist.
    #[error("all indexings blocked due to blocked POIs")]
    AllIndexingsBlockedByPoiBlocklist,

    /// The indexer's indexing progress resolution failed.
    #[error("indexing progress resolution failed: {0}")]
    IndexingProgressResolutionFailed(String),
    /// No indexing progress information was found for the indexer's deployments.
    #[error("no indexing progress information found")]
    IndexingProgressUnavailable,
}

impl From<HostResolutionError> for IndexerError {
    fn from(err: HostResolutionError) -> Self {
        IndexerError::HostResolutionFailed(err.to_string())
    }
}

impl From<PoiResolutionError> for IndexerError {
    fn from(err: PoiResolutionError) -> Self {
        IndexerError::IndexingPoisResolutionFailed(err.to_string())
    }
}

impl From<IndexingProgressResolutionError> for IndexerError {
    fn from(err: IndexingProgressResolutionError) -> Self {
        IndexerError::IndexingProgressResolutionFailed(err.to_string())
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexerIndexingError {
    /// The indexing has been blocked by the public POIs blocklist.
    #[error("indexing blocked by POIs blocklist")]
    BlockedByPoiBlocklist,

    /// The indexing progress information was not found.
    #[error("progress information not found")]
    ProgressNotFound,
}

/// Process the fetched network topology information.
pub async fn process_indexers_info(
    state: &InternalState,
    indexers: HashMap<Address, IndexerRawInfo>,
) -> HashMap<Address, Result<IndexerInfo, IndexerError>> {
    let processed_info = {
        let indexers_iter_fut = indexers.into_iter().map(move |(indexer_id, indexer)| {
            // Instrument the indexer processing span
            let indexer_span = tracing::debug_span!(
                "indexer processing",
                indexer.id = %indexer.id,
                indexer.url = %indexer.url,
                indexer.agent_version = tracing::field::Empty,
                indexer.graph_node_version = tracing::field::Empty,
            );
            tracing::trace!(parent: &indexer_span, "processing");

            async move {
                let indexer = indexer;

                // Check if the indexer's address is in the address blocklist
                if let Err(err) =
                    check_indexer_blocked_by_addr_blocklist(&state.indexer_addr_blocklist, &indexer)
                {
                    tracing::debug!(%err);
                    return (indexer_id, Err(err));
                }

                // Check if the indexer's host is in the host blocklist
                //
                // If the indexer host cannot be resolved or is in the blocklist, the indexer must
                // be marked as unhealthy
                if let Err(err) = resolve_and_check_indexer_blocked_by_host_blocklist(
                    &state.indexer_host_resolver,
                    &state.indexer_host_blocklist,
                    &indexer,
                )
                .await
                {
                    tracing::debug!(%err);
                    return (indexer_id, Err(err));
                }

                // Check if the indexer's reported versions are supported
                //
                // If the versions cannot be resolved or are not supported, the indexer must be
                // marked as unhealthy
                let (indexer_agent_version, graph_node_version) =
                    match resolve_and_check_indexer_blocked_by_version(
                        &state.indexer_version_resolver,
                        &state.indexer_min_agent_version,
                        &state.indexer_min_graph_node_version,
                        &indexer,
                    )
                    .await
                    {
                        Ok(versions) => versions,
                        Err(err) => {
                            tracing::debug!(%err);
                            return (indexer_id, Err(err));
                        }
                    };

                // Update the span information with the resolved versions
                tracing::Span::current()
                    .record(
                        "indexer.agent_version",
                        tracing::field::display(&indexer_agent_version),
                    )
                    .record(
                        "indexer.graph_node_version",
                        tracing::field::display(&graph_node_version),
                    );

                let mut indexer_indexings = indexer.indexings.keys().copied().collect::<Vec<_>>();

                // Check if the indexer's indexings should be blocked by POI
                let blocked_indexings_by_poi =
                    match resolve_and_check_indexer_indexings_blocked_by_poi(
                        &state.indexer_indexing_pois_blocklist,
                        &indexer_indexings,
                        &indexer,
                    )
                    .await
                    {
                        Ok(blocked_indexings) => blocked_indexings,
                        Err(err) => {
                            tracing::debug!(%err);
                            return (indexer_id, Err(err));
                        }
                    };

                // Update the indexer indexings list to only include the deployments that
                // are not blocked by POI. If all the indexer's indexings are blocked by POI,
                // mark the indexer as unhealthy.
                indexer_indexings.retain(|id| !blocked_indexings_by_poi.contains(id));
                if indexer_indexings.is_empty() {
                    return (
                        indexer_id,
                        Err(IndexerError::AllIndexingsBlockedByPoiBlocklist),
                    );
                }

                // Resolve the indexer's indexing progress information
                // NOTE: At this point, the indexer's deployments list should contain only the
                //       deployment IDs that were not blocked by any blocklist.
                let mut indexer_progress = match resolve_indexer_progress(
                    &state.indexer_indexing_progress_resolver,
                    &indexer_indexings,
                    &indexer,
                )
                .await
                {
                    Ok(progress) => progress,
                    Err(err) => {
                        tracing::debug!(%err);
                        return (indexer_id, Err(err));
                    }
                };

                // Update the indexer indexings list to only keep the indexings that have reported
                // successfully the progress information. If no progress information was found for
                // any of the indexer's deployments, mark the indexer as unhealthy.
                indexer_indexings.retain(|id| matches!(indexer_progress.get(id), Some(Ok(_))));
                if indexer_indexings.is_empty() {
                    return (indexer_id, Err(IndexerError::IndexingProgressUnavailable));
                }

                // Resolve the indexer's indexing cost models
                let mut indexer_cost_models = match resolve_indexer_cost_models(
                    &state.indexer_indexing_cost_model_resolver,
                    &indexer_indexings,
                    &indexer,
                )
                .await
                {
                    Ok(cost_models) => cost_models,
                    Err(_) => unreachable!(),
                };

                // Construct the indexer's information with the resolved information
                let info = IndexerInfo {
                    id: indexer.id,
                    url: indexer.url,
                    staked_tokens: indexer.staked_tokens,
                    deployments: indexer.deployments,
                    indexer_agent_version,
                    graph_node_version,
                    indexings: indexer
                        .indexings
                        .into_iter()
                        .map(|(id, info)| {
                            // Check if the indexing is blocked by POI
                            if blocked_indexings_by_poi.contains(&id) {
                                return (id, Err(IndexerIndexingError::BlockedByPoiBlocklist));
                            }

                            // Get the progress information
                            let progress = match indexer_progress
                                .remove(&id)
                                .expect("indexing progress not found")
                            {
                                Ok(progress) => progress,
                                Err(err) => return (id, Err(err)),
                            };

                            // Get the cost model
                            let cost_model = indexer_cost_models.remove(&id);

                            (
                                id,
                                Ok(IndexerIndexingInfo {
                                    largest_allocation: info.largest_allocation,
                                    total_allocated_tokens: info.total_allocated_tokens,
                                    progress,
                                    cost_model,
                                }),
                            )
                        })
                        .collect(),
                };

                (indexer_id, Ok(info))
            }
            .instrument(indexer_span)
        });

        // Wait for all the indexers to be processed
        futures::future::join_all(indexers_iter_fut).await
    };

    FromIterator::from_iter(processed_info)
}

/// Check if the indexer's address is in the address blocklist.
///
/// - If the address blocklist was not configured: the indexer is ALLOWED.
/// - If the address is in the blocklist: the indexer is BLOCKED.
fn check_indexer_blocked_by_addr_blocklist(
    blocklist: &Option<AddrBlocklist>,
    indexer: &IndexerRawInfo,
) -> Result<(), IndexerError> {
    let blocklist = match blocklist {
        Some(blocklist) => blocklist,
        None => return Ok(()),
    };

    // Check if the indexer's address is in the blocklist
    if blocklist.check(&indexer.id).is_blocked() {
        return Err(IndexerError::BlockedByAddrBlocklist);
    }

    Ok(())
}

/// Resolve and check if the indexer's host is in the host blocklist.
///
/// - If the indexer's host is not resolvable: the indexer is BLOCKED.
/// - If the host blocklist was not configured: the indexer is ALLOWED.
/// - If the indexer's host is in the blocklist: the indexer is BLOCKED.
async fn resolve_and_check_indexer_blocked_by_host_blocklist(
    resolver: &Mutex<HostResolver>,
    blocklist: &Option<HostBlocklist>,
    indexer: &IndexerRawInfo,
) -> Result<(), IndexerError> {
    // Resolve the indexer's URL, if it fails (or times out), the indexer must be BLOCKED
    let resolution_result = resolver.lock().await.resolve_url(&indexer.url).await?;

    // If the host blocklist was not configured, the indexer must be ALLOWED
    let host_blocklist = match blocklist {
        Some(blocklist) => blocklist,
        _ => return Ok(()),
    };

    if host_blocklist.check(&resolution_result).is_blocked() {
        return Err(IndexerError::BlockedByHostBlocklist);
    }

    Ok(())
}

/// Resolve and check if the indexer's reported versions are supported.
async fn resolve_and_check_indexer_blocked_by_version(
    resolver: &VersionResolver,
    min_agent_version: &Version,
    min_graph_node_version: &Version,
    indexer: &IndexerRawInfo,
) -> Result<(Version, Version), IndexerError> {
    // Resolve the indexer's agent version
    let agent_version = resolver
        .resolve_agent_version(&indexer.url)
        .await
        .map_err(|err| IndexerError::AgentVersionResolutionFailed(err.to_string()))?;

    // Check if the indexer's agent version is supported
    if agent_version < *min_agent_version {
        return Err(IndexerError::AgentVersionBelowMin(
            agent_version,
            min_agent_version.clone(),
        ));
    }

    // Resolve the indexer's graph node version, with a timeout
    let graph_node_version = match resolver.resolve_graph_node_version(&indexer.url).await {
        Err(err) => {
            // TODO: After more graph nodes support reporting their version,
            //  we should assume they are on the minimum version if we can't
            //  get the version.
            tracing::trace!("graph-node version resolution failed: {err}");
            min_graph_node_version.clone()
        }
        Ok(result) => result,
    };

    // Check if the indexer's graph node version is supported
    if graph_node_version < *min_graph_node_version {
        return Err(IndexerError::GraphNodeVersionBelowMin(
            graph_node_version,
            min_graph_node_version.clone(),
        ));
    }

    Ok((agent_version, graph_node_version))
}

/// Resolve and check if any of the indexer's deployments should be blocked by POI.
async fn resolve_and_check_indexer_indexings_blocked_by_poi(
    blocklist: &Option<(PoiBlocklist, Mutex<PoiResolver>)>,
    indexings: &[DeploymentId],
    indexer: &IndexerRawInfo,
) -> Result<HashSet<DeploymentId>, IndexerError> {
    // If the POI blocklist was not configured, the indexer must be ALLOWED
    let (pois_blocklist, pois_resolver) = match blocklist {
        Some((blocklist, resolver)) => (blocklist, resolver),
        _ => return Ok(HashSet::new()),
    };

    // Get the list of affected POIs to resolve for the indexer's deployments
    // If none of the deployments are affected, the indexer must be ALLOWED
    let indexer_affected_pois = pois_blocklist.affected_pois_metadata(&indexer.deployments);
    if indexer_affected_pois.is_empty() {
        return Ok(HashSet::new());
    }

    // Resolve the indexer public POIs for the affected deployments
    let poi_result = {
        let mut pois_resolver = pois_resolver.lock().await;
        pois_resolver
            .resolve(&indexer.url, &indexer_affected_pois)
            .await?
    };

    // Check if any of the reported POIs are in the blocklist
    let blocklist_check_result = pois_blocklist.check(poi_result);
    let blocked_indexings = indexings
        .iter()
        .filter_map(|id| match blocklist_check_result.get(id) {
            Some(state) if state.is_blocked() => Some(*id),
            _ => None,
        })
        .collect::<HashSet<_>>();

    Ok(blocked_indexings)
}

/// Resolve the indexer's progress information.
async fn resolve_indexer_progress(
    resolver: &IndexingProgressResolver,
    indexings: &[DeploymentId],
    indexer: &IndexerRawInfo,
) -> Result<HashMap<DeploymentId, Result<IndexingProgressInfo, IndexerIndexingError>>, IndexerError>
{
    let mut progress_info = resolver.resolve(&indexer.url, indexings).await?;
    tracing::trace!(
        indexings_requested = %indexer.deployments.len(),
        indexings_resolved = %progress_info.len(),
        "progress resolved"
    );

    let progress = indexings
        .iter()
        .map(|id| {
            (
                *id,
                progress_info
                    .remove(id)
                    .map(|res| IndexingProgressInfo {
                        latest_block: res.latest_block,
                        min_block: res.min_block,
                    })
                    .ok_or(IndexerIndexingError::ProgressNotFound),
            )
        })
        .collect();

    Ok(progress)
}

/// Resolve the indexer's cost models.
async fn resolve_indexer_cost_models(
    (resolver, compiler): &(CostModelResolver, Mutex<CostModelCompiler>),
    indexings: &[DeploymentId],
    indexer: &IndexerRawInfo,
) -> Result<HashMap<DeploymentId, Ptr<CostModel>>, Infallible> {
    // Resolve the indexer's cost model sources
    let cost_model_sources = match resolver.resolve(&indexer.url, indexings).await {
        Err(err) => {
            // If the resolution failed, return early
            tracing::trace!("cost model resolution failed: {err}");
            return Ok(HashMap::new());
        }
        Ok(result) if result.is_empty() => {
            // If the resolution is empty, return early
            return Ok(HashMap::new());
        }
        Ok(result) => result,
    };

    // Compile the cost model sources into cost models
    let cost_models = {
        let mut compiler = compiler.lock().await;
        cost_model_sources
            .into_iter()
            .filter_map(|(deployment, source)| match compiler.compile(source) {
                Err(err) => {
                    tracing::debug!("cost model compilation failed: {err}");
                    None
                }
                Ok(cost_model) => Some((deployment, cost_model)),
            })
            .collect()
    };

    Ok(cost_models)
}
