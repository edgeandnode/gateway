//! A resolver that fetches the indexing progress of deployments from an indexer's status URL.

use std::{collections::HashMap, time::Duration};

use alloy_primitives::BlockNumber;
use gateway_common::{caching::Freshness, ttl_hash_map::TtlHashMap};
use thegraph_core::types::DeploymentId;
use tokio::sync::RwLock;
use url::Url;

use crate::{indexers, indexers::indexing_statuses::IndexingStatusResponse};

/// The default timeout for the indexer's indexing progress resolution.
pub const DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(25);

/// The default TTL (time-to-live) for the cached indexer's indexing progress information: 10 minutes.
///
/// The cache TTL is the time that the indexer's indexing progress resolution is cached for.
pub const DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_CACHE_TTL: Duration =
    Duration::from_secs(60 * 10);

/// An error that occurred while resolving the indexer's progress.
// TODO: Differentiate deserialization errors from resolver errors
#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {
    /// An error occurred while fetching the indexer progress.
    ///
    /// This includes network errors, timeouts, and deserialization errors.
    #[error("fetch error: {0}")]
    FetchError(anyhow::Error),

    /// The resolution timed out.
    #[error("timeout")]
    Timeout,
}

/// The indexing progress information of a deployment on a chain.
#[derive(Debug, Clone)]
pub struct IndexingProgressInfo {
    /// The chain the deployment is associated with.
    pub chain: String,
    /// The latest block number indexed by the indexer.
    pub latest_block: BlockNumber,
    /// The earliest block number indexed by the indexer.
    pub min_block: Option<BlockNumber>,
}

/// A resolver that fetches the indexing progress of deployments from an indexer's status URL.
pub struct IndexingProgressResolver {
    client: reqwest::Client,
    timeout: Duration,
    cache: RwLock<TtlHashMap<(String, DeploymentId), IndexingProgressInfo>>,
}

impl IndexingProgressResolver {
    /// Creates a new [`IndexingProgressResolver`].
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            timeout: DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT,
            cache: RwLock::new(TtlHashMap::with_ttl(
                DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_CACHE_TTL,
            )),
        }
    }

    /// Creates a new [`IndexingProgressResolver`] with the given timeout and cache TTL.
    pub fn with_timeout_and_cache_ttl(
        client: reqwest::Client,
        timeout: Duration,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            client,
            timeout,
            cache: RwLock::new(TtlHashMap::with_ttl(cache_ttl)),
        }
    }

    /// Fetches the indexing progress of the given deployments from the indexer's status URL.
    async fn fetch_indexing_progress(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> Result<Vec<IndexingStatusResponse>, ResolutionError> {
        let indexer_status_url = indexers::status_url(url);
        tokio::time::timeout(
            self.timeout,
            // TODO: Handle the different errors once the indexers client module reports them
            indexers::indexing_statuses::query(&self.client, indexer_status_url, indexings),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(ResolutionError::FetchError)
    }

    /// Gets the cached progress information for the given indexings.
    ///
    /// This method locks the cache in read mode and returns the cached progress information for the
    /// given indexings.
    async fn get_from_cache(
        &self,
        url: &str,
        indexings: impl IntoIterator<Item = &DeploymentId>,
    ) -> HashMap<DeploymentId, IndexingProgressInfo> {
        let read_cache = self.cache.read().await;
        let mut result = HashMap::new();

        for deployment in indexings {
            match read_cache.get(&(url.to_owned(), *deployment)) {
                Some(data) => {
                    result.insert(*deployment, data.clone());
                }
                None => continue,
            }
        }

        result
    }

    /// Updates the cache with the given progress information.
    ///
    /// This method locks the cache in write mode and updates the cache with the given progress
    /// information.
    async fn update_cache(
        &self,
        url: &str,
        progress: &HashMap<DeploymentId, IndexingProgressInfo>,
    ) {
        let mut write_cache = self.cache.write().await;
        for (deployment, data) in progress.iter() {
            write_cache.insert((url.to_owned(), *deployment), data.clone());
        }
    }

    /// Resolves the indexing progress of the given deployments.
    ///
    /// If the request successfully returns the data, the cached data is updated and the new data is
    /// returned, otherwise the cached data is returned.
    async fn resolve_with_cache(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> Result<HashMap<DeploymentId, Freshness<IndexingProgressInfo>>, ResolutionError> {
        let url_string = url.to_string();

        let fetched = match self.fetch_indexing_progress(url, indexings).await {
            Ok(fetched) => fetched,
            Err(err) => {
                tracing::debug!(error=%err, "indexing progress fetch failed");

                // If the data fetch failed, return the cached data
                // If no cached data is available, return the error
                let cached_progress = self
                    .get_from_cache(&url_string, indexings)
                    .await
                    .into_iter()
                    .map(|(k, v)| (k, Freshness::Cached(v)))
                    .collect::<HashMap<_, _>>();
                return if cached_progress.is_empty() {
                    Err(err)
                } else {
                    Ok(cached_progress)
                };
            }
        };

        let fresh_progress = fetched
            .into_iter()
            .filter_map(|resp| {
                // Only consider the first chain status
                let chain = resp.chains.into_iter().next()?;

                // If the status has no chains or no latest block, skip it
                let info_chain = chain.network;
                let info_latest_block = chain.latest_block.map(|block| block.number)?;
                let info_min_block = chain.earliest_block.as_ref().map(|block| block.number);

                Some((
                    resp.subgraph,
                    IndexingProgressInfo {
                        chain: info_chain,
                        latest_block: info_latest_block,
                        min_block: info_min_block,
                    },
                ))
            })
            .collect::<HashMap<_, _>>();

        // Update the cache with the fetched data, if any
        if !fresh_progress.is_empty() {
            self.update_cache(&url_string, &fresh_progress).await;
        }

        // Get the cached data for the missing deployments
        let cached_progress = {
            // Get the list of deployments that are missing from the fetched data
            let missing_indexings = fresh_progress
                .keys()
                .filter(|deployment| !indexings.contains(deployment));

            // Get the cached data for the missing deployments
            self.get_from_cache(&url_string, missing_indexings).await
        };

        // Merge the fetched and cached data
        let fresh_progress = fresh_progress
            .into_iter()
            .map(|(k, v)| (k, Freshness::Fresh(v)));
        let cached_progress = cached_progress
            .into_iter()
            .map(|(k, v)| (k, Freshness::Cached(v)));
        Ok(HashMap::from_iter(cached_progress.chain(fresh_progress)))
    }
    /// Resolves the indexing progress of the given deployments.
    ///
    /// Returns a map of deployment IDs to their indexing progress information.
    pub async fn resolve(
        &self,
        url: &Url,
        indexer_deployments: &[DeploymentId],
    ) -> Result<HashMap<DeploymentId, Freshness<IndexingProgressInfo>>, ResolutionError> {
        self.resolve_with_cache(url, indexer_deployments).await
    }
}
