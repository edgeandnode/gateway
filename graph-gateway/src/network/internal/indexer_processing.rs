use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
};

use alloy_primitives::{Address, BlockNumber};
use cost_model::CostModel;
use custom_debug::CustomDebug;
use gateway_common::{blocklist::Blocklist as _, caching::Freshness, ptr::Ptr};
use semver::Version;
use thegraph_core::types::DeploymentId;
use tokio::sync::Mutex;
use tracing::Instrument;
use url::Url;

use crate::network::{
    indexer_addr_blocklist::AddrBlocklist,
    indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::{HostResolver, ResolutionError as HostResolutionError},
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_poi_blocklist::PoiBlocklist,
    indexer_indexing_poi_resolver::{PoiResolver, ResolutionError as PoisResolutionError},
    indexer_indexing_progress_resolver::{
        IndexingProgressResolver, ResolutionError as IndexingProgressResolutionError,
    },
    indexer_version_resolver::{ResolutionError as VersionResolutionError, VersionResolver},
};

/// The minimum version requirements for the indexer.
#[derive(Debug, Clone)]
pub struct VersionRequirements {
    /// The minimum indexer  version.
    pub min_indexer_service_version: Version,
    /// The minimum graph node version.
    pub min_graph_node_version: Version,
}

impl Default for VersionRequirements {
    fn default() -> Self {
        Self {
            min_indexer_service_version: Version::new(0, 0, 0),
            min_graph_node_version: Version::new(0, 0, 0),
        }
    }
}

/// Internal representation of the indexer pre-processed information.
///
/// This is not the final representation of the indexer.
#[derive(Clone, CustomDebug)]
pub(super) struct IndexerRawInfo {
    /// The indexer's ID.
    pub id: Address,
    /// The indexer's URL.
    ///
    /// It is guaranteed that the URL scheme is either HTTP or HTTPS and the URL has a host.
    #[debug(with = std::fmt::Display::fmt)]
    pub url: Url,
    /// The total amount of tokens staked by the indexer.
    pub staked_tokens: u128,
    /// The indexer's indexings information.
    pub indexings: HashMap<DeploymentId, IndexerIndexingRawInfo>,
}

/// Internal representation of the indexer's indexing information.
///
/// This is not the final representation of the indexer's indexing information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IndexerIndexingRawInfo {
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
    /// The indexer's "indexer service" version.
    pub indexer_service_version: Version,
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
    pub progress: Freshness<IndexingProgressInfo>,

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

/// Errors when processing the indexer information.
#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexerError {
    /// The indexer was blocked by the address blocklist.
    #[error("indexer address blocked by blocklist")]
    BlockedByAddrBlocklist,

    /// The indexer's host resolution failed.
    #[error("indexer host resolution failed: {0}")]
    HostResolutionFailed(#[from] HostResolutionError),
    /// The indexer was blocked by the host blocklist.
    #[error("indexer host blocked by blocklist")]
    BlockedByHostBlocklist,

    /// The indexer's service version resolution failed.
    #[error("indexer service version resolution failed: {0}")]
    IndexerServiceVersionResolutionFailed(VersionResolutionError),
    /// The indexer's service version is below the minimum required.
    #[error("service version {0} below the minimum required {1}")]
    IndexerServiceVersionBelowMin(Version, Version),

    /// The indexer's graph node version resolution failed.
    #[error("graph-node version resolution failed: {0}")]
    GraphNodeVersionResolutionFailed(VersionResolutionError),
    /// The indexer's graph node version is below the minimum required.
    #[error("graph node version {0} below the minimum required {1}")]
    GraphNodeVersionBelowMin(Version, Version),

    /// The indexer's indexing public POIs resolution failed.
    #[error("indexing public POIs resolution failed: {0}")]
    IndexingPoisResolutionFailed(#[from] PoisResolutionError),
    /// All the indexer's indexings are blocked by the public POIs blocklist.
    #[error("all indexings blocked due to blocked POIs")]
    AllIndexingsBlockedByPoiBlocklist,

    /// The indexer's indexing progress resolution failed.
    #[error("indexing progress resolution failed: {0}")]
    IndexingProgressResolutionFailed(#[from] IndexingProgressResolutionError),
    /// No indexing progress information was found for the indexer's deployments.
    #[error("no indexing progress information found")]
    IndexingProgressUnavailable,
}

/// Error when processing the indexer's indexing information.
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
pub(super) async fn process_info<S>(
    state: &S,
    indexers: HashMap<Address, IndexerRawInfo>,
) -> HashMap<Address, Result<IndexerInfo, IndexerError>>
where
    S: AsRef<Option<AddrBlocklist>>
        + AsRef<Mutex<HostResolver>>
        + AsRef<Option<HostBlocklist>>
        + AsRef<VersionRequirements>
        + AsRef<VersionResolver>
        + AsRef<Option<(PoiBlocklist, Mutex<PoiResolver>)>>
        + AsRef<IndexingProgressResolver>
        + AsRef<(CostModelResolver, Mutex<CostModelCompiler>)>,
{
    let processed_info = {
        let indexers_iter_fut = indexers.into_iter().map(move |(indexer_id, indexer)| {
            // Instrument the indexer processing span
            let indexer_span = tracing::debug_span!(
                "indexer processing",
                indexer.id = ?indexer.id,
                indexer.url = %indexer.url,
                indexer.indexer_service_version = tracing::field::Empty,
                indexer.graph_node_version = tracing::field::Empty,
            );
            tracing::trace!(parent: &indexer_span, "processing");

            async move {
                let indexer = indexer;

                // Check if the indexer's address is in the address blocklist
                if let Err(err) = check_indexer_blocked_by_addr_blocklist(state.as_ref(), &indexer)
                {
                    tracing::debug!(%err);
                    return (indexer_id, Err(err));
                }

                // Check if the indexer's host is in the host blocklist
                //
                // If the indexer host cannot be resolved or is in the blocklist, the indexer must
                // be marked as unhealthy
                if let Err(err) = resolve_and_check_indexer_blocked_by_host_blocklist(
                    state.as_ref(),
                    state.as_ref(),
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
                let (indexer_service_version, graph_node_version) =
                    match resolve_and_check_indexer_blocked_by_version(
                        state.as_ref(),
                        state.as_ref(),
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
                        "indexer.indexer_service_version",
                        tracing::field::display(&indexer_service_version),
                    )
                    .record(
                        "indexer.graph_node_version",
                        tracing::field::display(&graph_node_version),
                    );

                let mut indexer_indexings = indexer.indexings.keys().copied().collect::<Vec<_>>();

                // Check if the indexer's indexings should be blocked by POI
                let blocked_indexings_by_poi =
                    match resolve_and_check_indexer_indexings_blocked_by_poi(
                        state.as_ref(),
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
                let mut indexer_progress =
                    match resolve_indexer_progress(state.as_ref(), &indexer_indexings, &indexer)
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
                let mut indexer_cost_models =
                    match resolve_indexer_cost_models(state.as_ref(), &indexer_indexings, &indexer)
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
                    indexer_service_version,
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
    version_requirements: &VersionRequirements,
    resolver: &VersionResolver,
    indexer: &IndexerRawInfo,
) -> Result<(Version, Version), IndexerError> {
    // Resolve the indexer's service version
    let indexer_service_version = resolver
        .resolve_indexer_service_version(&indexer.url)
        .await
        .map_err(IndexerError::IndexerServiceVersionResolutionFailed)?;

    // Check if the indexer's service version is supported
    if indexer_service_version < version_requirements.min_indexer_service_version {
        return Err(IndexerError::IndexerServiceVersionBelowMin(
            indexer_service_version,
            version_requirements.min_indexer_service_version.clone(),
        ));
    }

    // Resolve the indexer's graph node version, with a timeout
    let graph_node_version = match resolver.resolve_graph_node_version(&indexer.url).await {
        Err(err) => {
            // TODO: After more graph nodes support reporting their version,
            //  we should assume they are on the minimum version if we can't
            //  get the version.
            tracing::trace!("graph-node version resolution failed: {err}");
            version_requirements.min_graph_node_version.clone()
        }
        Ok(result) => result,
    };

    // Check if the indexer's graph node version is supported
    if graph_node_version < version_requirements.min_graph_node_version {
        return Err(IndexerError::GraphNodeVersionBelowMin(
            graph_node_version,
            version_requirements.min_graph_node_version.clone(),
        ));
    }

    Ok((indexer_service_version, graph_node_version))
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
    let indexer_affected_pois = pois_blocklist.affected_pois_metadata(indexer.indexings.keys());
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
) -> Result<
    HashMap<DeploymentId, Result<Freshness<IndexingProgressInfo>, IndexerIndexingError>>,
    IndexerError,
> {
    let mut progress_info = resolver.resolve(&indexer.url, indexings).await?;
    tracing::trace!(
        indexings_requested = %indexings.len(),
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
                    .map(|res| {
                        res.map(|data| IndexingProgressInfo {
                            latest_block: data.latest_block,
                            min_block: data.min_block,
                        })
                    })
                    .ok_or(IndexerIndexingError::ProgressNotFound),
            )
        })
        .collect::<HashMap<_, _>>();

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
            .filter_map(
                |(deployment, source)| match compiler.compile(source.as_ref()) {
                    Err(err) => {
                        tracing::debug!("cost model compilation failed: {err}");
                        None
                    }
                    Ok(cost_model) => Some((deployment, cost_model)),
                },
            )
            .collect()
    };

    Ok(cost_models)
}
