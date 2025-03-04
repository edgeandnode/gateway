use std::collections::{HashMap, HashSet};

use custom_debug::CustomDebug;
use thegraph_core::{AllocationId, DeploymentId, IndexerId, alloy::primitives::BlockNumber};
use tracing::Instrument;
use url::Url;

use crate::{
    errors::UnavailableReason,
    network::{indexing_progress::IndexingProgressResolver, service::InternalState},
};

/// Internal representation of the indexer pre-processed information.
///
/// This is not the final representation of the indexer.
#[derive(CustomDebug)]
pub struct IndexerRawInfo {
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
pub struct IndexerInfo<I> {
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
    pub indexings: HashMap<DeploymentId, Result<I, UnavailableReason>>,
}

/// Internal representation of the indexer's indexing information.
///
/// This is not the final representation of the indexer's indexing information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexingRawInfo {
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
pub struct IndexingInfo<P, C> {
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

pub type ResolvedIndexingInfo = IndexingInfo<IndexingProgress, u128>;

pub type ResolvedIndexerInfo = IndexerInfo<ResolvedIndexingInfo>;

/// Internal representation of the indexing's progress information.
#[derive(Clone, Debug)]
pub struct IndexingProgress {
    /// The latest indexed block.
    pub latest_block: BlockNumber,
    /// The minimum indexed block.
    pub min_block: Option<BlockNumber>,
}

/// Process the fetched network topology information.
pub async fn process_info(
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
            );
            tracing::trace!(parent: &indexer_span, "processing");

            async move {
                if let Err(err) = state.indexer_host_filter.check(&indexer.url).await {
                    return (*indexer_id, Err(err));
                }
                if let Err(err) = state.indexer_version_filter.check(&indexer.url).await {
                    return (*indexer_id, Err(err));
                }

                let blocklist = state
                    .indexer_blocklist
                    .borrow()
                    .get(&*indexer.id)
                    .cloned()
                    .unwrap_or_default();

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

/// Process the indexer's indexings information.
async fn process_indexer_indexings(
    state: &InternalState,
    url: &Url,
    indexings: HashMap<DeploymentId, IndexingRawInfo>,
    blocklist: HashSet<DeploymentId>,
) -> HashMap<DeploymentId, Result<ResolvedIndexingInfo, UnavailableReason>> {
    let mut indexer_indexings: HashMap<DeploymentId, Result<IndexingInfo<(), ()>, _>> = indexings
        .into_iter()
        .map(|(id, info)| (id, Ok(info.into())))
        .collect();

    for deployment in blocklist {
        indexer_indexings.insert(
            deployment,
            Err(UnavailableReason::Blocked("missing data".to_string())),
        );
    }

    // ref: df8e647b-1e6e-422a-8846-dc9ee7e0dcc2
    let status_url = url.join("status").unwrap();

    // Keep track of the healthy indexers, so we efficiently resolve the indexer's indexings thar
    // are not marked as unhealthy in a previous resolution step
    let mut healthy_indexer_indexings = indexer_indexings.keys().copied().collect::<Vec<_>>();

    // Check if the indexer's indexings should be blocked by POI
    let blocked_indexings_by_poi = state
        .indexer_poi_filer
        .blocked_deployments(&status_url, &healthy_indexer_indexings)
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
        &status_url,
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

/// Resolve the indexer's progress information.
async fn resolve_indexer_progress(
    resolver: &IndexingProgressResolver,
    status_url: &Url,
    indexings: &[DeploymentId],
) -> HashMap<DeploymentId, Result<IndexingProgress, UnavailableReason>> {
    let mut progress_info = resolver.resolve(status_url, indexings).await;

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
