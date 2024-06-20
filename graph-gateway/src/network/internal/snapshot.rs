//! Entities that are used to represent the network topology.

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{Arc, OnceLock},
};

use alloy_primitives::{Address, BlockNumber};
use cost_model::CostModel;
use custom_debug::CustomDebug;
use gateway_common::ptr::Ptr;
use semver::Version;
use thegraph_core::types::{DeploymentId, SubgraphId};
use url::Url;

use super::{DeploymentInfo, SubgraphInfo};
use crate::network::{
    errors::{DeploymentError, IndexerInfoResolutionError, IndexingError, SubgraphError},
    internal::indexer_processing::ResolvedIndexerInfo,
};

/// The minimum indexer service version required to support Scalar TAP.
fn min_required_indexer_service_version_tap_support() -> &'static Version {
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

#[derive(Debug, Clone)]
pub struct Indexing {
    /// The indexing unique identifier.
    pub id: IndexingId,
    /// The indexing chain.
    pub chain: String,
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
    /// See [`IndexingProgress`] for more information.
    pub progress: IndexingProgress,
    /// The indexer's indexing cost model
    pub cost_model: Option<Ptr<CostModel>>,
}

/// The [`IndexingProgress`] struct represents the progress of an indexing.
#[derive(Debug, Clone)]
pub struct IndexingProgress {
    /// The latest block the indexer has indexed for the deployment.
    pub latest_block: BlockNumber,
    /// The minimum block the indexer has indexed for the deployment.
    pub min_block: Option<BlockNumber>,
}

impl IndexingProgress {
    /// Returns the reported indexed range.
    ///
    /// The range is a tuple of the minimum block and the latest block the indexer has reported
    /// as indexed.
    pub fn as_range(&self) -> (Option<BlockNumber>, BlockNumber) {
        (self.min_block, self.latest_block)
    }
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
    #[debug(with = std::fmt::Display::fmt)]
    pub url: Url,

    /// The indexer's "indexer service" version.
    pub indexer_service_version: Version,
    /// The indexer's "graph node" version.
    pub graph_node_version: Version,

    /// Whether the indexer supports TAP payments.
    pub tap_support: bool,

    /// The indexer's staked tokens.
    pub staked_tokens: u128,
}

#[derive(Debug, Clone)]
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
    /// Subgraph versions, in descending order.
    pub versions: Vec<DeploymentId>,
    /// The subgraph's indexings.
    ///
    /// A table holding all the known indexings for the subgraph.
    pub indexings: HashMap<IndexingId, Result<Indexing, IndexingError>>,
}

#[derive(Debug, Clone)]
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

/// A snapshot of the network topology.
#[derive(Default)]
pub struct NetworkTopologySnapshot {
    /// Subgraphs network topology table.
    pub subgraphs: HashMap<SubgraphId, Result<Subgraph, SubgraphError>>,
    /// Deployments network topology table.
    pub deployments: HashMap<DeploymentId, Result<Deployment, DeploymentError>>,
}

/// Construct the [`NetworkTopologySnapshot`] from the indexers and subgraphs information.
pub fn new_from(
    indexers_info: HashMap<Address, Result<ResolvedIndexerInfo, IndexerInfoResolutionError>>,
    subgraphs_info: HashMap<SubgraphId, Result<SubgraphInfo, SubgraphError>>,
    deployments_info: HashMap<DeploymentId, Result<DeploymentInfo, DeploymentError>>,
) -> NetworkTopologySnapshot {
    // Construct the indexers table
    let indexers = indexers_info
        .into_iter()
        .map(|(indexer_id, indexer)| {
            (
                indexer_id,
                indexer.map(|info| {
                    // The indexer service version must be greater than or equal to the minimum
                    // required version to support Scalar TAP.
                    let indexer_tap_support = &info.indexer_service_version
                        >= min_required_indexer_service_version_tap_support();

                    let indexer = Indexer {
                        id: info.id,
                        url: info.url.clone(),
                        indexer_service_version: info.indexer_service_version.clone(),
                        graph_node_version: info.graph_node_version.clone(),
                        tap_support: indexer_tap_support,
                        staked_tokens: info.staked_tokens,
                    };

                    (info, Arc::new(indexer))
                }),
            )
        })
        .collect::<HashMap<_, _>>();

    // Construct the subgraphs table
    let subgraphs = subgraphs_info
        .into_iter()
        .map(|(id, info)| {
            (
                id,
                info.and_then(|info| construct_subgraphs_table_row(info, &indexers)),
            )
        })
        .collect();

    // Construct the deployments table
    let deployments = deployments_info
        .into_iter()
        .map(|(id, info)| {
            (
                id,
                info.and_then(|info| construct_deployments_table_row(info, &indexers)),
            )
        })
        .collect();

    NetworkTopologySnapshot {
        deployments,
        subgraphs,
    }
}

/// Construct the subgraphs table row.
fn construct_subgraphs_table_row(
    subgraph_info: SubgraphInfo,
    indexers: &HashMap<
        Address,
        Result<(ResolvedIndexerInfo, Arc<Indexer>), IndexerInfoResolutionError>,
    >,
) -> Result<Subgraph, SubgraphError> {
    let versions = subgraph_info.versions;
    let version_ids = versions.iter().map(|v| v.deployment_id).collect();

    // As versions are ordered in descending order, the first version is the highest
    // If all the subgraph's versions are invalid, exclude the subgraph.
    let highest_version = versions
        .iter()
        .find(|version| version.deployment.is_ok())
        .expect("no valid versions found");

    let (
        highest_version_deployment_manifest_chain,
        highest_version_deployment_manifest_start_block,
    ) = {
        let deployment = highest_version
            .deployment
            .as_ref()
            .expect("invalid deployment");

        (
            deployment.manifest_network.to_owned(),
            deployment.manifest_start_block,
        )
    };

    // Construct the subgraph's indexings table
    // Invalid versions are excluded from the indexings table, i.e., if the version deployment is
    // invalid, the version is filtered out and not included in the indexings table.
    let subgraph_indexings = versions
        .into_iter()
        .filter_map(|version| version.deployment.ok())
        .filter(|deployment| {
            // Make sure we only select deployments indexing the same chain
            // This simplifies dealing with block constraints later
            deployment.manifest_network == highest_version_deployment_manifest_chain
        })
        .flat_map(|deployment| {
            deployment
                .allocations
                .into_iter()
                .map(|alloc| {
                    let indexing_id = IndexingId {
                        indexer: alloc.indexer,
                        deployment: deployment.id,
                    };
                    construct_indexings_table_row(
                        indexing_id,
                        &deployment.manifest_network,
                        indexers,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<HashMap<_, _>>();

    // If all the subgraph's versions are invalid, mark the subgraph as invalid.
    if subgraph_indexings.is_empty() {
        return Err(SubgraphError::NoValidVersions);
    }

    Ok(Subgraph {
        id: subgraph_info.id,
        chain: highest_version_deployment_manifest_chain,
        start_block: highest_version_deployment_manifest_start_block,
        versions: version_ids,
        indexings: subgraph_indexings,
    })
}

/// Construct the subgraphs table row.
fn construct_deployments_table_row(
    deployment_info: DeploymentInfo,
    indexers: &HashMap<
        Address,
        Result<(ResolvedIndexerInfo, Arc<Indexer>), IndexerInfoResolutionError>,
    >,
) -> Result<Deployment, DeploymentError> {
    let deployment_id = deployment_info.id;
    let deployment_manifest_chain = deployment_info.manifest_network;
    let deployment_manifest_start_block = deployment_info.manifest_start_block;

    let deployment_indexings = deployment_info
        .allocations
        .into_iter()
        .map(|alloc| {
            let indexing_id = IndexingId {
                indexer: alloc.indexer,
                deployment: deployment_id,
            };

            construct_indexings_table_row(indexing_id, &deployment_manifest_chain, indexers)
        })
        .collect::<HashMap<_, _>>();
    if deployment_indexings.is_empty() {
        return Err(DeploymentError::NoAllocations);
    }

    let deployment_subgraphs = deployment_info.subgraphs;

    Ok(Deployment {
        id: deployment_id,
        chain: deployment_manifest_chain,
        start_block: deployment_manifest_start_block,
        subgraphs: deployment_subgraphs,
        indexings: deployment_indexings,
    })
}

/// Construct the indexing table row.
///
/// If the indexer reported an error for the indexing, the row is constructed with the error.
fn construct_indexings_table_row(
    indexing_id: IndexingId,
    indexing_deployment_chain: &str,
    indexers: &HashMap<
        Address,
        Result<(ResolvedIndexerInfo, Arc<Indexer>), IndexerInfoResolutionError>,
    >,
) -> (IndexingId, Result<Indexing, IndexingError>) {
    // If the indexer reported an error, bail out.
    let (indexer_info, indexer) = match indexers.get(&indexing_id.indexer).as_ref() {
        Some(Ok(indexer)) => indexer,
        Some(Err(err)) => return (indexing_id, Err(err.clone().into())),
        None => {
            // Log this error as it should not happen.
            tracing::error!(
                indexer = %indexing_id.indexer,
                deployment = %indexing_id.deployment,
                "indexing indexer info not found"
            );

            return (
                indexing_id,
                Err(IndexingError::Internal("indexer not found")),
            );
        }
    };

    // If the indexer's indexing info is not found or failed to resolve, bail out.
    let indexing_info = match indexer_info.indexings.get(&indexing_id.deployment) {
        Some(Ok(info)) => info,
        Some(Err(err)) => return (indexing_id, Err(err.clone().into())),
        None => {
            // Log this error as it should not happen.
            tracing::error!(
                indexer = %indexing_id.indexer,
                deployment = %indexing_id.deployment,
                "indexing info not found"
            );

            return (
                indexing_id,
                Err(IndexingError::Internal("indexing info not found")),
            );
        }
    };

    // Construct the indexing table row
    let indexing_largest_allocation_addr = indexing_info.largest_allocation;
    let indexing_total_allocated_tokens = indexing_info.total_allocated_tokens;
    let indexing_progress = indexing_info.progress.to_owned();
    let indexing_cost_model = indexing_info.cost_model.to_owned();

    let indexing = Indexing {
        id: indexing_id,
        chain: indexing_deployment_chain.to_owned(),
        largest_allocation: indexing_largest_allocation_addr,
        total_allocated_tokens: indexing_total_allocated_tokens,
        indexer: Arc::clone(indexer),
        progress: IndexingProgress {
            latest_block: indexing_progress.latest_block,
            min_block: indexing_progress.min_block,
        },
        cost_model: indexing_cost_model,
    };

    (indexing_id, Ok(indexing))
}
