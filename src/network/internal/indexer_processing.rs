use std::collections::{HashMap, HashSet};

use custom_debug::CustomDebug;
use semver::Version;
use thegraph_core::{alloy::primitives::BlockNumber, AllocationId, DeploymentId, IndexerId};
use tracing::Instrument;
use url::Url;

use super::InternalState;
use crate::{
    config::BlockedIndexer,
    errors::UnavailableReason,
    network::{
        config::VersionRequirements, indexer_indexing_poi_blocklist::PoiBlocklist,
        indexer_indexing_poi_resolver::PoiResolver,
        indexer_indexing_progress_resolver::IndexingProgressResolver,
        indexer_version_resolver::VersionResolver,
    },
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
    pub indexings: HashMap<DeploymentId, Result<I, UnavailableReason>>,
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

    /// The indexer fee for this indexing.
    pub fee: C,
}

impl From<IndexingRawInfo> for IndexingInfo<(), ()> {
    fn from(raw: IndexingRawInfo) -> Self {
        Self {
            largest_allocation: raw.largest_allocation,
            total_allocated_tokens: raw.total_allocated_tokens,
            progress: (),
            fee: (),
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
            fee: self.fee,
        }
    }
}

impl IndexingInfo<IndexingProgress, ()> {
    /// Move the type-state-machine from the partially resolved state to the completely resolved
    /// state by adding the cost model to the indexing information.
    fn with_fee(self, fee: u128) -> IndexingInfo<IndexingProgress, u128> {
        IndexingInfo {
            largest_allocation: self.largest_allocation,
            total_allocated_tokens: self.total_allocated_tokens,
            progress: self.progress,
            fee,
        }
    }
}

pub(super) type ResolvedIndexingInfo = IndexingInfo<IndexingProgress, u128>;

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
) -> HashMap<IndexerId, Result<ResolvedIndexerInfo, UnavailableReason>> {
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
                if let Err(err) = state.indexer_host_filter.check(&indexer.url).await {
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

                let blocklist = state.indexer_blocklist.get(&*indexer.id);
                // Resolve the indexer's indexings information
                let indexings = process_indexer_indexings(
                    state,
                    &indexer.url,
                    indexer.indexings.clone(),
                    blocklist,
                )
                .await;

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

/// Resolve and check if the indexer's reported versions are supported.
async fn resolve_and_check_indexer_blocked_by_version(
    resolver: &VersionResolver,
    version_requirements: &VersionRequirements,
    url: &Url,
) -> Result<(Version, Version), UnavailableReason> {
    // Resolve the indexer's service version
    let indexer_service_version = resolver
        .resolve_indexer_service_version(url)
        .await
        .map_err(|err| UnavailableReason::NoStatus(format!("failed to reach indexer: {err}")))?;

    // Check if the indexer's service version is supported
    if indexer_service_version < version_requirements.min_indexer_service_version {
        return Err(UnavailableReason::NotSupported(
            "indexer-service version below minimum".to_string(),
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
        return Err(UnavailableReason::NotSupported(
            "graph-node version below minimum".to_string(),
        ));
    }

    Ok((indexer_service_version, graph_node_version))
}

/// Process the indexer's indexings information.
async fn process_indexer_indexings(
    state: &InternalState,
    url: &Url,
    indexings: HashMap<DeploymentId, IndexingRawInfo>,
    blocklist: Option<&BlockedIndexer>,
) -> HashMap<DeploymentId, Result<ResolvedIndexingInfo, UnavailableReason>> {
    let mut indexer_indexings: HashMap<DeploymentId, Result<IndexingInfo<(), ()>, _>> = indexings
        .into_iter()
        .map(|(id, info)| (id, Ok(info.into())))
        .collect();

    match blocklist {
        None => (),
        Some(blocklist) if blocklist.deployments.is_empty() => {
            for entry in indexer_indexings.values_mut() {
                *entry = Err(UnavailableReason::Blocked(blocklist.reason.clone()));
            }
        }
        Some(blocklist) => {
            for deployment in &blocklist.deployments {
                indexer_indexings.insert(
                    *deployment,
                    Err(UnavailableReason::Blocked(blocklist.reason.clone())),
                );
            }
        }
    };

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
                    Err(UnavailableReason::Blocked("bad PoI".to_string()))
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
                        Err(UnavailableReason::NoStatus(
                            "failed to get indexing progress".to_string(),
                        )),
                    );
                }
            };

            (id, Ok(info.with_indexing_progress(progress)))
        })
        .collect::<HashMap<_, _>>();

    // Resolve the indexer's indexing cost models
    let mut indexer_cost_models = state
        .cost_model_resolver
        .resolve(url, &healthy_indexer_indexings)
        .await;

    // Update the indexer's indexings info with the cost models
    indexer_indexings
        .into_iter()
        .map(|(id, res)| {
            let info = match res {
                Ok(info) => info,
                Err(err) => return (id, Err(err)),
            };
            let fee = indexer_cost_models.remove(&id).unwrap_or(0);
            (id, Ok(info.with_fee(fee)))
        })
        .collect()
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
) -> HashMap<DeploymentId, Result<IndexingProgress, UnavailableReason>> {
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
                    .ok_or_else(|| {
                        UnavailableReason::NoStatus("failed to get indexing progress".to_string())
                    }),
            )
        })
        .collect::<HashMap<_, _>>()
}
