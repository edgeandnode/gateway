//! Entities that are used to represent the network topology.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    ops::Deref,
    sync::{Arc, OnceLock},
};

pub use alloy_primitives::{Address, BlockNumber};
use cost_model::CostModel;
use custom_debug::CustomDebug;
use eventuals::Ptr;
use semver::Version;
pub use thegraph_core::types::{DeploymentId, SubgraphId};
use url::Url;

use super::internal::{
    types::{DeploymentInfo, IndexerInfo, SubgraphInfo},
    IndexerError as InternalIndexerError, IndexerError,
    IndexerIndexingError as InternalIndexerIndexingError, IndexerIndexingError,
};

/// The minimum indexer agent version required to support Scalar TAP.
fn min_required_indexer_agent_version_scalar_tap_support() -> &'static Version {
    static VERSION: OnceLock<Version> = OnceLock::new();
    VERSION.get_or_init(|| "1.0.0-alpha".parse().expect("valid version"))
}

/// The [`IndexingId`] struct represents the unique identifier of an indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingId {
    /// The indexer's ID indexing the subgraph's deployment.
    pub indexer: Address,
    /// The subgraph's deployment ID indexed by the indexer.
    pub deployment: DeploymentId,
}

#[derive(Clone)]
pub struct Indexing {
    /// The indexing unique identifier.
    pub id: IndexingId,

    /// The versions behind the highest version of the subgraph being indexed.
    pub versions_behind: u8,

    /// The largest allocation address.
    ///
    /// This is, among all allocations associated with the indexer and deployment, the address
    /// with the largest amount of allocated tokens.
    pub largest_allocation: Address,
    /// The indexer's indexing total allocated tokens.
    ///
    /// This is, the sum of all allocated tokens associated with the indexer and deployment.
    pub total_allocated_tokens: u128,

    /// The indexer
    pub indexer: Arc<Indexer>,

    /// The indexing progress.
    ///
    /// See [`IndexationProgress`] for more information.
    pub progress: IndexationProgress,
    /// The indexer's indexing cost model
    pub cost_model: Option<Ptr<CostModel>>,
}

/// The [`IndexationProgress`] struct represents the progress of an indexing.
#[derive(Debug, Clone)]
pub struct IndexationProgress {
    /// The latest block the indexer has indexed for the deployment.
    pub latest_block: BlockNumber,
    /// The minimum block the indexer has indexed for the deployment.
    pub min_block: Option<BlockNumber>,
}

/// The [`Indexer`] struct represents an indexer in the network topology.
///
/// The indexer is a network  node that indexes a subgraph using one of its versions, i.e., one of
/// the subgraph's deployments. The [`Indexing`] struct represents the indexer's indexing of a
/// subgraph's deployment.
#[derive(CustomDebug, Clone)]
pub struct Indexer {
    /// The indexer's ID.
    pub id: Address,

    /// The indexer's URL.
    ///
    /// It is guaranteed that the URL scheme is either HTTP or HTTPS and the URL has a host.
    #[debug(with = Display::fmt)]
    pub url: Url,

    /// The indexer's "indexer service" version.
    pub indexer_agent_version: Version,
    /// The indexer's "graph node" version.
    pub graph_node_version: Version,

    /// Whether the indexer supports using Scalar TAP.
    pub scalar_tap_support: bool,

    /// The indexer's indexings set.
    ///
    /// It is a set of deployment IDs that the indexer is indexing.
    pub indexings: HashSet<DeploymentId>,

    /// The indexer's staked tokens.
    pub staked_tokens: u128,
}

#[derive(Clone)]
pub struct Subgraph {
    /// Subgraph ID
    pub id: SubgraphId,

    /// The subgraph's chain name.
    ///
    /// This information is extracted from the highest version of the subgraph deployment's
    /// manifest.
    pub chain: String,
    /// The subgraph's start block number.
    ///
    /// This information is extracted from the highest version of the subgraph deployment's
    /// manifest.
    pub start_block: BlockNumber,

    /// The subgraph's deployments.
    ///
    /// A list of deployment IDs known to be healthy and currently serving queries.
    pub deployments: HashSet<DeploymentId>,

    /// The subgraph's indexings.
    ///
    /// A table holding all the known indexings for the subgraph.
    pub indexings: HashMap<IndexingId, Result<Indexing, IndexingError>>,
}

#[derive(Clone)]
pub struct Deployment {
    /// Deployment ID.
    ///
    /// The IPFS content ID of the subgraph manifest.
    pub id: DeploymentId,

    /// The deployment chain name.
    ///
    /// This field is extracted from the deployment manifest.
    pub chain: String,
    /// The deployment start block number.
    ///
    /// This field is extracted from the deployment manifest.
    pub start_block: BlockNumber,

    /// A deployment may be associated with multiple subgraphs.
    pub subgraphs: HashSet<SubgraphId>,

    /// The deployment's indexings.
    ///
    /// A table holding all the known indexings for the deployment.
    pub indexings: HashMap<IndexingId, Result<Indexing, IndexingError>>,
}

// TODO: Review these errors when the network module gets integrated
//  Copied from gateway-framework/src/errors.rs
#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexingError {
    /// Errors that should only occur in exceptional conditions.
    #[error("internal error: {0}")]
    Internal(String),
    /// The indexer is considered unavailable.
    #[error("Unavailable({0})")]
    Unavailable(UnavailableReason),
}

impl From<InternalIndexerError> for IndexingError {
    fn from(err: InternalIndexerError) -> Self {
        IndexingError::Unavailable(match err {
            IndexerError::BlockedByAddrBlocklist => UnavailableReason::BlockedByAddrBlocklist,
            IndexerError::HostResolutionFailed(_) => UnavailableReason::NoStatus(err.to_string()),
            IndexerError::BlockedByHostBlocklist => UnavailableReason::BlockedByHostBlocklist,
            IndexerError::AgentVersionResolutionFailed(_) => {
                UnavailableReason::NoStatus(err.to_string())
            }
            IndexerError::AgentVersionBelowMin(cur, min) => {
                UnavailableReason::AgentVersionBelowMin(cur, min)
            }
            IndexerError::GraphNodeVersionResolutionFailed(_) => {
                UnavailableReason::NoStatus(err.to_string())
            }
            IndexerError::GraphNodeVersionBelowMin(cur, min) => {
                UnavailableReason::GraphNodeVersionBelowMin(cur, min)
            }
            IndexerError::IndexingPoisResolutionFailed(_) => {
                UnavailableReason::NoStatus(err.to_string())
            }
            IndexerError::AllIndexingsBlockedByPoiBlocklist => {
                UnavailableReason::IndexingBlockedByPoiBlocklist
            }
            IndexerError::IndexingProgressResolutionFailed(_) => {
                UnavailableReason::NoStatus(err.to_string())
            }
            IndexerError::IndexingProgressUnavailable => {
                UnavailableReason::NoStatus(err.to_string())
            }
        })
    }
}

impl From<InternalIndexerIndexingError> for IndexingError {
    fn from(err: InternalIndexerIndexingError) -> Self {
        IndexingError::Unavailable(match err {
            IndexerIndexingError::BlockedByPoiBlocklist => {
                UnavailableReason::IndexingBlockedByPoiBlocklist
            }
            IndexerIndexingError::ProgressNotFound => UnavailableReason::NoStatus(err.to_string()),
        })
    }
}

// TODO: Review these errors when the network module gets integrated
//  Copied from gateway-framework/src/errors.rs
#[derive(Clone, Debug, thiserror::Error)]
pub enum UnavailableReason {
    /// Blocked by address blocklist.
    #[error("blocked by address blocklist")]
    BlockedByAddrBlocklist,
    /// Blocked by host blocklist.
    #[error("blocked by host blocklist")]
    BlockedByHostBlocklist,
    /// Indexer agent version is below the minimum required version.
    #[error("indexer agent version below the minimum required version")]
    AgentVersionBelowMin(Version, Version),
    /// Graph node version is below the minimum required version.
    #[error("graph node version below the minimum required version")]
    GraphNodeVersionBelowMin(Version, Version),
    /// All indexings are blocked by the POI blocklist.
    #[error("indexing blocked by POI blocklist")]
    IndexingBlockedByPoiBlocklist,
    /// Failed to resolve indexer information
    #[error("no status")]
    NoStatus(String),
}

/// A snapshot of the network topology.
pub struct NetworkTopologySnapshot {
    /// Table holding the subgraph ID of the transferred subgraphs and the L2 subgraph ID.
    transferred_subgraphs: HashMap<SubgraphId, SubgraphId>,
    /// Table holding the deployment ID of the transferred deployments.
    transferred_deployments: HashSet<DeploymentId>,

    /// Subgraphs network topology table.
    subgraphs: HashMap<SubgraphId, Subgraph>,
    /// Deployments network topology table.
    deployments: HashMap<DeploymentId, Deployment>,
}

impl NetworkTopologySnapshot {
    /// Get the [`Subgraph`] by [`SubgraphId`].
    ///
    /// If the subgraph is not found, it returns `None`.
    pub fn get_subgraph_by_id(&self, id: &SubgraphId) -> Option<&Subgraph> {
        self.subgraphs.get(id)
    }

    /// Get the [`Deployment`] by [`DeploymentId`].
    ///
    /// If the deployment is not found, it returns `None`.
    pub fn get_deployment_by_id(&self, id: &DeploymentId) -> Option<&Deployment> {
        self.deployments.get(id)
    }

    /// Get the snapshot subgraphs.
    pub fn subgraphs(&self) -> impl Deref<Target = HashMap<SubgraphId, Subgraph>> + '_ {
        &self.subgraphs
    }

    /// Get the snapshot deployments.
    pub fn deployments(&self) -> impl Deref<Target = HashMap<DeploymentId, Deployment>> + '_ {
        &self.deployments
    }

    /// Get the snapshot transferred subgraphs.
    pub fn transferred_subgraphs(
        &self,
    ) -> impl Deref<Target = HashMap<SubgraphId, SubgraphId>> + '_ {
        &self.transferred_subgraphs
    }

    /// Get the snapshot transferred deployments.
    pub fn transferred_deployments(&self) -> impl Deref<Target = HashSet<DeploymentId>> + '_ {
        &self.transferred_deployments
    }
}

/// Construct the [`NetworkTopologySnapshot`] from the indexers and subgraphs information.
pub fn new_from(
    indexers_info: HashMap<Address, Result<IndexerInfo, InternalIndexerError>>,
    subgraphs_info: HashMap<SubgraphId, SubgraphInfo>,
) -> NetworkTopologySnapshot {
    // Construct the indexers table
    let indexers = indexers_info
        .into_iter()
        .map(|(indexer_id, indexer)| {
            (
                indexer_id,
                indexer.map(|info| {
                    // The indexer agent version must be greater than or equal to the minimum
                    // required version to support Scalar TAP.
                    let indexer_scalar_tap_support = &info.indexer_agent_version
                        >= min_required_indexer_agent_version_scalar_tap_support();

                    let indexer = Indexer {
                        id: info.id,
                        url: info.url.clone(),
                        indexer_agent_version: info.indexer_agent_version.clone(),
                        graph_node_version: info.graph_node_version.clone(),
                        scalar_tap_support: indexer_scalar_tap_support,
                        indexings: info.indexings.keys().copied().collect(),
                        staked_tokens: info.staked_tokens,
                    };

                    (info, Arc::new(indexer))
                }),
            )
        })
        .collect::<HashMap<_, _>>();

    // Construct the deployments table
    let deployments_info = subgraphs_info
        .values()
        .flat_map(|subgraph| {
            subgraph
                .versions
                .iter()
                .map(|version| (version.deployment.id, version.deployment.clone()))
        })
        .collect::<HashMap<_, _>>();

    // Construct the transferred subgraphs and deployments tables
    let transferred_subgraphs = construct_transferred_subgraphs_table(&subgraphs_info);
    let transferred_deployments = construct_transferred_deployments_table(&deployments_info);

    // Construct the subgraphs table
    let subgraphs = subgraphs_info
        .into_iter()
        .filter(|(id, _)| {
            let transferred_to_l2 = transferred_subgraphs.contains_key(id);
            if transferred_to_l2 {
                tracing::debug!("filtering subgraphs table row: transferred to L2");
            }

            // Keep the subgraph if it is not transferred to L2
            !transferred_to_l2
        })
        .filter_map(|entry| {
            match try_construct_subgraphs_table_row(entry, &indexers, &transferred_deployments) {
                Ok(row) => Some(row),
                Err(err) => {
                    tracing::debug!("filtering subgraphs table row: {err}");
                    None
                }
            }
        })
        .collect();

    // Construct the deployments table
    let deployments = deployments_info
        .into_iter()
        .filter(|(deployment_id, _)| {
            let transferred_to_l2 = transferred_deployments.contains(deployment_id);
            if transferred_to_l2 {
                tracing::debug!("filtering deployments table row: transferred to L2");
            }

            // Keep the deployment if it is not transferred to L2
            !transferred_to_l2
        })
        .filter_map(|entry| {
            match try_construct_deployments_table_row(entry, &indexers, &subgraphs) {
                Ok(row) => Some(row),
                Err(err) => {
                    tracing::debug!("filtering deployments table row: {err}");
                    None
                }
            }
        })
        .collect();

    NetworkTopologySnapshot {
        transferred_subgraphs,
        transferred_deployments,
        deployments,
        subgraphs,
    }
}

/// Extracts from the subgraphs info table the subgraph IDs that:
/// - All its versions-deployments are marked as transferred to L2.
/// - All its versions-deployments have no allocations.
fn construct_transferred_subgraphs_table(
    subgraphs_info: &HashMap<SubgraphId, SubgraphInfo>,
) -> HashMap<SubgraphId, SubgraphId> {
    subgraphs_info
        .iter()
        .filter_map(|(subgraph_id, subgraph)| {
            // A subgraph is considered to be transferred to L2 if all its versions-deployments
            // are transferred to L2 (i.e., `transferred_to_l2` is `true`) and have no allocations.
            let transferred_to_l2 = subgraph.versions.iter().all(|version| {
                version.deployment.transferred_to_l2 && version.deployment.allocations.is_empty()
            });

            // If the subgraph is transferred to L2 and has an ID on L2, return the pair.
            // Otherwise, exclude the subgraph.
            if transferred_to_l2 && subgraph.id_on_l2.is_some() {
                Some((*subgraph_id, subgraph.id_on_l2?))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>()
}

/// Extracts from the deployments info table the deployment IDs that:
///  - Are marked as transferred to L2.
///  - Have no associated allocations.
fn construct_transferred_deployments_table(
    deployments_info: &HashMap<DeploymentId, DeploymentInfo>,
) -> HashSet<DeploymentId> {
    deployments_info
        .iter()
        .filter_map(|(deployment_id, deployment)| {
            if deployment.transferred_to_l2 && deployment.allocations.is_empty() {
                Some(*deployment_id)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>()
}

/// Construct the subgraphs table row.
fn try_construct_subgraphs_table_row(
    (subgraph_id, subgraph_info): (SubgraphId, SubgraphInfo),
    indexers: &HashMap<Address, Result<(IndexerInfo, Arc<Indexer>), InternalIndexerError>>,
    transferred_deployments: &HashSet<DeploymentId>,
) -> anyhow::Result<(SubgraphId, Subgraph)> {
    // Filter-out the subgraphs' invalid versions-deployments.
    let versions = subgraph_info
        .versions
        .into_iter()
        .filter(|version| {
            // Valid version must have a deployment with:
            // - Valid manifest info (i.e., network).
            // - Not marked as transferred to L2.
            version.deployment.manifest_network.is_some()
                && !transferred_deployments.contains(&version.deployment.id)
        })
        .collect::<Vec<_>>();

    // As versions are ordered in descending order, the first version is the highest
    // If all the subgraph's versions are invalid, exclude the subgraph.
    let highest_version = versions
        .first()
        .ok_or_else(|| anyhow::anyhow!("no valid versions"))?;

    let highest_version_number = highest_version.version;
    let highest_version_deployment_manifest_chain = highest_version
        .deployment
        .manifest_network
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no deployment manifest network"))?
        .clone();
    let highest_version_deployment_manifest_start_block =
        highest_version.deployment.manifest_start_block.unwrap_or(0);

    let versions_behind_table = versions
        .iter()
        .map(|version| {
            let deployment_id = version.deployment.id;
            let deployment_versions_behind = highest_version_number
                .saturating_sub(version.version)
                .try_into()
                .unwrap_or(u8::MAX);
            (deployment_id, deployment_versions_behind)
        })
        .collect::<HashMap<_, _>>();

    let subgraph_indexings = versions
        .into_iter()
        .flat_map(|version| {
            let deployment_id = version.deployment.id;
            let indexing_deployment_versions_behind = versions_behind_table
                .get(&deployment_id)
                .copied()
                .unwrap_or(u8::MAX);

            version
                .deployment
                .allocations
                .into_iter()
                .map(|alloc| {
                    let indexing_id = IndexingId {
                        indexer: alloc.indexer,
                        deployment: deployment_id,
                    };

                    construct_indexings_table_row(
                        indexing_id,
                        indexing_deployment_versions_behind,
                        indexers,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<HashMap<_, _>>();
    if subgraph_indexings.is_empty() {
        return Err(anyhow::anyhow!("no indexings"));
    }

    let subgraph_deployments = subgraph_indexings
        .keys()
        .map(|indexing_id| indexing_id.deployment)
        .collect::<HashSet<_>>();

    Ok((
        subgraph_id,
        Subgraph {
            id: subgraph_info.id,
            chain: highest_version_deployment_manifest_chain,
            start_block: highest_version_deployment_manifest_start_block,
            deployments: subgraph_deployments,
            indexings: subgraph_indexings,
        },
    ))
}

/// Construct the subgraphs table row.
fn try_construct_deployments_table_row(
    (deployment_id, deployment_info): (DeploymentId, DeploymentInfo),
    indexers: &HashMap<Address, Result<(IndexerInfo, Arc<Indexer>), InternalIndexerError>>,
    subgraphs: &HashMap<SubgraphId, Subgraph>,
) -> anyhow::Result<(DeploymentId, Deployment)> {
    let deployment_versions_behind = 0;
    let deployment_manifest_chain = deployment_info
        .manifest_network
        .ok_or_else(|| anyhow::anyhow!("no deployment manifest network"))?
        .clone();
    let deployment_manifest_start_block = deployment_info.manifest_start_block.unwrap_or(0);

    let deployment_indexings = deployment_info
        .allocations
        .into_iter()
        .map(|alloc| {
            let indexing_id = IndexingId {
                indexer: alloc.indexer,
                deployment: deployment_id,
            };

            construct_indexings_table_row(indexing_id, deployment_versions_behind, indexers)
        })
        .collect::<HashMap<_, _>>();
    if deployment_indexings.is_empty() {
        return Err(anyhow::anyhow!("no indexings"));
    }

    let deployment_subgraphs = subgraphs
        .iter()
        .filter_map(|(subgraph_id, subgraph)| {
            if subgraph.deployments.contains(&deployment_id) {
                Some(*subgraph_id)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();
    if deployment_subgraphs.is_empty() {
        return Err(anyhow::anyhow!("no subgraphs"));
    }

    Ok((
        deployment_id,
        Deployment {
            id: deployment_id,
            chain: deployment_manifest_chain,
            start_block: deployment_manifest_start_block,
            subgraphs: deployment_subgraphs,
            indexings: deployment_indexings,
        },
    ))
}

/// Construct the indexing table row.
///
/// If the indexer reported an error for the indexing, the row is constructed with the error.
fn construct_indexings_table_row(
    indexing_id: IndexingId,
    indexing_deployment_versions_behind: u8,
    indexers: &HashMap<Address, Result<(IndexerInfo, Arc<Indexer>), InternalIndexerError>>,
) -> (IndexingId, Result<Indexing, IndexingError>) {
    // If the indexer reported an error, bail out.
    let (indexer_info, indexer) = match indexers.get(&indexing_id.indexer).as_ref() {
        Some(Ok(indexer)) => indexer,
        Some(Err(err)) => return (indexing_id, Err(err.clone().into())),
        None => {
            return (
                indexing_id,
                Err(IndexingError::Internal("indexer not found".to_string())),
            )
        }
    };

    // If the indexer's indexing info is not found or failed to resolve, bail out.
    let indexing_info = match indexer_info.indexings.get(&indexing_id.deployment) {
        Some(Ok(info)) => info,
        Some(Err(err)) => return (indexing_id, Err(err.clone().into())),
        None => {
            return (
                indexing_id,
                Err(IndexingError::Internal(
                    "indexing info not found".to_string(),
                )),
            )
        }
    };

    // Construct the indexing table row
    let indexing_largest_allocation_addr = indexing_info.largest_allocation;
    let indexing_total_allocated_tokens = indexing_info.total_allocated_tokens;
    let indexing_progress = indexing_info.progress.to_owned();
    let indexing_cost_model = indexing_info.cost_model.to_owned();

    let indexing = Indexing {
        id: indexing_id,
        versions_behind: indexing_deployment_versions_behind,
        largest_allocation: indexing_largest_allocation_addr,
        total_allocated_tokens: indexing_total_allocated_tokens,
        indexer: Arc::clone(indexer),
        progress: IndexationProgress {
            latest_block: indexing_progress.latest_block,
            min_block: indexing_progress.min_block,
        },
        cost_model: indexing_cost_model,
    };

    (indexing_id, Ok(indexing))
}
