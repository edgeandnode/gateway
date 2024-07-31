use std::collections::{HashMap, HashSet};

use alloy_primitives::BlockNumber;
use cost_model::CostModel;
use custom_debug::CustomDebug;
use gateway_common::ptr::Ptr;
use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::types::{AllocationId, DeploymentId, IndexerId};
use tracing::Instrument;
use url::Url;

use super::InternalState;
use crate::network::{
    config::VersionRequirements,
    errors::{IndexerInfoResolutionError, IndexingInfoResolutionError},
    indexer_host_resolver::HostResolver,
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_poi_blocklist::PoiBlocklist,
    indexer_indexing_poi_resolver::PoiResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::VersionResolver,
};

/// Internal representation of the indexer pre-processed information.
///
/// This is not the final representation of the indexer.
#[derive(CustomDebug)]
pub(super) struct IndexerRawInfo {
    /// The indexer's ID.
    pub id: IndexerId,
    /// The indexer's URL.
    ///
    /// It is guaranteed that the URL scheme is either HTTP or HTTPS and the URL has a host.
    #[debug(with = std::fmt::Display::fmt)]
    pub url: Url,
    /// The total amount of tokens staked by the indexer.
    pub staked_tokens: u128,
    /// The indexer's indexings information.
    pub indexings: HashMap<DeploymentId, IndexingRawInfo>,
}

/// Internal representation of the fetched indexer information.
///
/// This is not the final representation of the indexer.
#[derive(CustomDebug)]
pub(super) struct IndexerInfo<I> {
    /// The indexer's ID.
    pub id: IndexerId,
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
    pub indexings: HashMap<DeploymentId, Result<I, IndexingInfoResolutionError>>,
}

/// Internal representation of the indexer's indexing information.
///
/// This is not the final representation of the indexer's indexing information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IndexingRawInfo {
    /// The largest allocation.
    pub largest_allocation: AllocationId,
    /// The total amount of tokens allocated.
    pub total_allocated_tokens: u128,
}

/// Internal representation of the fetched indexer's indexing information.
///
/// This type uses the "type-state" pattern to represent the different states of the indexing
/// information: unresolved, partially resolved (with indexing progress) and completely resolved.
#[derive(Debug)]
pub(super) struct IndexingInfo<P, C> {
    /// The largest allocation.
    pub largest_allocation: AllocationId,

    /// The total amount of tokens allocated.
    pub total_allocated_tokens: u128,

    /// The indexing progress information
    ///
    /// See [`IndexingProgress`] for more information.
    pub progress: P, // Freshness<IndexingProgressInfo>,

    /// The cost model for this indexing.
    pub cost_model: C, // Option<Ptr<CostModel>>,
}

impl From<IndexingRawInfo> for IndexingInfo<(), ()> {
    fn from(raw: IndexingRawInfo) -> Self {
        Self {
            largest_allocation: raw.largest_allocation,
            total_allocated_tokens: raw.total_allocated_tokens,
            progress: (),
            cost_model: (),
        }
    }
}

impl IndexingInfo<(), ()> {
    /// Move the type-state-machine from the unresolved state to the partially resolved state by
    /// adding the indexing progress information.
    fn with_indexing_progress(
        self,
        progress: IndexingProgress,
    ) -> IndexingInfo<IndexingProgress, ()> {
        IndexingInfo {
            largest_allocation: self.largest_allocation,
            total_allocated_tokens: self.total_allocated_tokens,
            progress,
            cost_model: self.cost_model,
        }
    }
}

impl IndexingInfo<IndexingProgress, ()> {
    /// Move the type-state-machine from the partially resolved state to the completely resolved
    /// state by adding the cost model to the indexing information.
    fn with_cost_model(
        self,
        cost_model: Option<Ptr<CostModel>>,
    ) -> IndexingInfo<IndexingProgress, Option<Ptr<CostModel>>> {
        IndexingInfo {
            largest_allocation: self.largest_allocation,
            total_allocated_tokens: self.total_allocated_tokens,
            progress: self.progress,
            cost_model,
        }
    }
}

pub(super) type ResolvedIndexingInfo = IndexingInfo<IndexingProgress, Option<Ptr<CostModel>>>;

pub(super) type ResolvedIndexerInfo = IndexerInfo<ResolvedIndexingInfo>;

/// Internal representation of the indexing's progress information.
#[derive(Clone, Debug)]
pub(super) struct IndexingProgress {
    /// The latest indexed block.
    pub latest_block: BlockNumber,
    /// The minimum indexed block.
    pub min_block: Option<BlockNumber>,
}

/// Process the fetched network topology information.
pub(super) async fn process_info(
    state: &InternalState,
    indexers: &HashMap<IndexerId, IndexerRawInfo>,
) -> HashMap<IndexerId, Result<ResolvedIndexerInfo, IndexerInfoResolutionError>> {
    let processed_info = {
        let indexers_iter_fut = indexers.iter().map(move |(indexer_id, indexer)| {
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
                if state.indexer_addr_blocklist.contains(indexer_id) {
                    return (
                        *indexer_id,
                        Err(IndexerInfoResolutionError::BlockedByAddrBlocklist),
                    );
                }

                // Check if the indexer's host is in the host blocklist
                //
                // If the indexer host cannot be resolved or is in the blocklist, the indexer must
                // be marked as unhealthy
                if let Err(err) = resolve_and_check_indexer_blocked_by_host_blocklist(
                    &state.indexer_host_resolver,
                    &state.indexer_host_blocklist,
                    &indexer.url,
                )
                .await
                {
                    tracing::debug!(%err);
                    return (*indexer_id, Err(err));
                }

                // Check if the indexer's reported versions are supported
                //
                // If the versions cannot be resolved or are not supported, the indexer must be
                // marked as unhealthy
                let (indexer_service_version, graph_node_version) =
                    match resolve_and_check_indexer_blocked_by_version(
                        &state.indexer_version_resolver,
                        &state.indexer_version_requirements,
                        &indexer.url,
                    )
                    .await
                    {
                        Ok(versions) => versions,
                        Err(err) => {
                            tracing::debug!(%err);
                            return (*indexer_id, Err(err));
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

                // Resolve the indexer's indexings information
                let indexings =
                    process_indexer_indexings(state, &indexer.url, indexer.indexings.clone()).await;

                (
                    *indexer_id,
                    Ok(IndexerInfo {
                        id: indexer.id,
                        url: indexer.url.clone(),
                        staked_tokens: indexer.staked_tokens,
                        indexer_service_version,
                        graph_node_version,
                        indexings,
                    }),
                )
            }
            .instrument(indexer_span)
        });

        // Wait for all the indexers to be processed
        futures::future::join_all(indexers_iter_fut).await
    };

    FromIterator::from_iter(processed_info)
}

/// Resolve and check if the indexer's host is in the host blocklist.
///
/// - If the indexer's host is not resolvable: the indexer is BLOCKED.
/// - If the host blocklist was not configured: the indexer is ALLOWED.
/// - If the indexer's host is in the blocklist: the indexer is BLOCKED.
async fn resolve_and_check_indexer_blocked_by_host_blocklist(
    resolver: &HostResolver,
    blocklist: &HashSet<IpNetwork>,
    url: &Url,
) -> Result<(), IndexerInfoResolutionError> {
    // Resolve the indexer's URL, if it fails (or times out), the indexer must be BLOCKED
    let addrs = resolver.resolve_url(url).await?;

    if addrs
        .iter()
        .any(|addr| blocklist.iter().any(|net| net.contains(*addr)))
    {
        return Err(IndexerInfoResolutionError::BlockedByHostBlocklist);
    }

    Ok(())
}

/// Resolve and check if the indexer's reported versions are supported.
async fn resolve_and_check_indexer_blocked_by_version(
    resolver: &VersionResolver,
    version_requirements: &VersionRequirements,
    url: &Url,
) -> Result<(Version, Version), IndexerInfoResolutionError> {
    // Resolve the indexer's service version
    let indexer_service_version = resolver
        .resolve_indexer_service_version(url)
        .await
        .map_err(IndexerInfoResolutionError::IndexerServiceVersionResolutionFailed)?;

    // Check if the indexer's service version is supported
    if indexer_service_version < version_requirements.min_indexer_service_version {
        return Err(IndexerInfoResolutionError::IndexerServiceVersionBelowMin(
            indexer_service_version,
            version_requirements.min_indexer_service_version.clone(),
        ));
    }

    // Resolve the indexer's graph node version, with a timeout
    let graph_node_version = match resolver.resolve_graph_node_version(url).await {
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
        return Err(IndexerInfoResolutionError::GraphNodeVersionBelowMin(
            graph_node_version,
            version_requirements.min_graph_node_version.clone(),
        ));
    }

    Ok((indexer_service_version, graph_node_version))
}

/// Process the indexer's indexings information.
async fn process_indexer_indexings(
    state: &InternalState,
    url: &Url,
    indexings: HashMap<DeploymentId, IndexingRawInfo>,
) -> HashMap<DeploymentId, Result<ResolvedIndexingInfo, IndexingInfoResolutionError>> {
    let indexer_indexings: HashMap<DeploymentId, Result<IndexingInfo<(), ()>, _>> = indexings
        .into_iter()
        .map(|(id, info)| (id, Ok(info.into())))
        .collect();

    // Keep track of the healthy indexers, so we efficiently resolve the indexer's indexings thar
    // are not marked as unhealthy in a previous resolution step
    let mut healthy_indexer_indexings = indexer_indexings.keys().copied().collect::<Vec<_>>();

    // Check if the indexer's indexings should be blocked by POI
    let blocked_indexings_by_poi = resolve_and_check_indexer_indexings_blocked_by_poi(
        &state.poi_blocklist,
        &state.poi_resolver,
        url,
        &healthy_indexer_indexings,
    )
    .await;

    // Remove the blocked indexings from the healthy indexers list
    healthy_indexer_indexings.retain(|id| !blocked_indexings_by_poi.contains(id));

    // Mark the indexings blocked by the POI blocklist as unhealthy
    let indexer_indexings = indexer_indexings
        .into_iter()
        .map(|(id, res)| {
            let info = res.and_then(|info| {
                if blocked_indexings_by_poi.contains(&id) {
                    Err(IndexingInfoResolutionError::BlockedByPoiBlocklist)
                } else {
                    Ok(info)
                }
            });

            (id, info)
        })
        .collect::<HashMap<_, _>>();

    // Resolve the indexer's indexing progress information
    let mut indexing_progress = resolve_indexer_progress(
        &state.indexing_progress_resolver,
        url,
        &healthy_indexer_indexings,
    )
    .await;

    // Remove the indexings with no indexing progress information from the healthy indexers list
    healthy_indexer_indexings.retain(|id| indexing_progress.contains_key(id));

    // Update the indexer's indexings with the progress information. If the progress information
    // is not found, the indexer must be marked as unhealthy
    let indexer_indexings = indexer_indexings
        .into_iter()
        .map(|(id, res)| {
            let info = match res {
                Ok(info) => info,
                Err(err) => return (id, Err(err)),
            };

            let progress = match indexing_progress.remove(&id) {
                Some(Ok(progress)) => progress,
                _ => {
                    return (
                        id,
                        Err(IndexingInfoResolutionError::IndexingProgressNotFound),
                    );
                }
            };

            (id, Ok(info.with_indexing_progress(progress)))
        })
        .collect::<HashMap<_, _>>();

    // Resolve the indexer's indexing cost models
    let mut indexer_cost_models = resolve_indexer_cost_models(
        &state.cost_model_resolver,
        &state.cost_model_compiler,
        url,
        &healthy_indexer_indexings,
    )
    .await;

    // Update the indexer's indexings info with the cost models
    let indexer_indexings = indexer_indexings
        .into_iter()
        .map(|(id, res)| {
            let info = match res {
                Ok(info) => info,
                Err(err) => return (id, Err(err)),
            };

            let cost_model = indexer_cost_models.remove(&id);

            (id, Ok(info.with_cost_model(cost_model)))
        })
        .collect::<HashMap<_, _>>();

    // Return the processed indexer's indexings
    #[allow(clippy::let_and_return)]
    indexer_indexings
}

/// Resolve and check if any of the indexer's indexings should be blocked by POI.
async fn resolve_and_check_indexer_indexings_blocked_by_poi(
    blocklist: &PoiBlocklist,
    resolver: &PoiResolver,
    url: &Url,
    indexings: &[DeploymentId],
) -> HashSet<DeploymentId> {
    if blocklist.is_empty() {
        return Default::default();
    }

    // Get the list of affected POIs to resolve for the indexer's deployments
    // If none of the deployments are affected, the indexer must be ALLOWED
    let indexer_affected_pois = blocklist.affected_pois_metadata(indexings);
    if indexer_affected_pois.is_empty() {
        return Default::default();
    }

    // Resolve the indexer public POIs for the affected deployments
    let poi_result = resolver.resolve(url, &indexer_affected_pois).await;

    blocklist.check(poi_result)
}

/// Resolve the indexer's progress information.
async fn resolve_indexer_progress(
    resolver: &IndexingProgressResolver,
    url: &Url,
    indexings: &[DeploymentId],
) -> HashMap<DeploymentId, Result<IndexingProgress, IndexingInfoResolutionError>> {
    let mut progress_info = resolver.resolve(url, indexings).await;

    // Get the progress information for each indexing
    indexings
        .iter()
        .map(|id| {
            (
                *id,
                progress_info
                    .remove(id)
                    .map(|info| IndexingProgress {
                        latest_block: info.latest_block,
                        min_block: info.min_block,
                    })
                    .ok_or(IndexingInfoResolutionError::IndexingProgressNotFound),
            )
        })
        .collect::<HashMap<_, _>>()
}

/// Resolve the indexer's cost models.
async fn resolve_indexer_cost_models(
    resolver: &CostModelResolver,
    compiler: &CostModelCompiler,
    url: &Url,
    indexings: &[DeploymentId],
) -> HashMap<DeploymentId, Ptr<CostModel>> {
    // Resolve the indexer's cost model sources
    let cost_model_sources = match resolver.resolve(url, indexings).await {
        Err(err) => {
            // If the resolution failed, return early
            tracing::trace!("cost model resolution failed: {err}");
            return HashMap::new();
        }
        Ok(result) if result.is_empty() => {
            // If the resolution is empty, return early
            return HashMap::new();
        }
        Ok(result) => result,
    };

    // Compile the cost model sources into cost models
    let cost_models = cost_model_sources
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
        .collect();

    cost_models
}
