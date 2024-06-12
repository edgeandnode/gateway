//! Resolves the cost models for the indexers' deployments.
//!
//! The cost models are fetched from the indexer's cost URL.

use std::{collections::HashMap, time::Duration};

use gateway_common::{ptr::Ptr, ttl_hash_map::TtlHashMap};
use thegraph_core::types::DeploymentId;
use tokio::sync::RwLock;
use url::Url;

use crate::{
    indexers,
    indexers::cost_models::{CostModelSource, Error as IndexerCostModelFetchError},
};

/// The default timeout for the indexer indexings' cost model resolution.
pub const DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);

/// The default TTL (time-to-live) for the cached indexer's cost model sources: 5 minutes.
///
/// The cache TTL is the maximum time that the cached cost model sources are valid.
pub const DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_CACHE_TTL: Duration =
    Duration::from_secs(300);

/// Error that can occur during cost model resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {
    /// Cost model fetch failed.
    #[error("fetch error: {0}")]
    FetchError(#[from] IndexerCostModelFetchError),

    /// Resolution timed out.
    #[error("timeout")]
    Timeout,
}

/// Resolve the indexers' cost models sources and compile them into cost models.
// TODO: Cache the resolution result with TTL in case the resolution fails.
pub struct CostModelResolver {
    client: reqwest::Client,
    timeout: Duration,
    cache: RwLock<TtlHashMap<(String, DeploymentId), Ptr<CostModelSource>>>,
}

impl CostModelResolver {
    /// Creates a new [`CostModelResolver`] with the given HTTP client.
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            timeout: DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT,
            cache: RwLock::new(TtlHashMap::with_ttl(
                DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_CACHE_TTL,
            )),
        }
    }

    /// Creates a new [`CostModelResolver`] with the given HTTP client, timeout, and cache TTL.
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

    async fn fetch_cost_model_sources(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> Result<Vec<CostModelSource>, ResolutionError> {
        let indexer_cost_url = indexers::cost_url(url);
        tokio::time::timeout(
            self.timeout,
            indexers::cost_models::send_request(&self.client, indexer_cost_url, indexings),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(ResolutionError::FetchError)
    }

    /// Gets the cached cost model sources for the given indexings.
    ///
    /// This method locks the cache in read mode and returns the cached data for the given URL and
    /// given indexings.
    async fn get_from_cache(
        &self,
        url: &str,
        indexings: impl IntoIterator<Item = &DeploymentId>,
    ) -> HashMap<DeploymentId, Ptr<CostModelSource>> {
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

    /// Updates the cache with the given cost model sources.
    ///
    /// This method locks the cache in write mode and updates the cache with the given progress
    /// information.
    async fn update_cache(
        &self,
        url: &str,
        progress: &HashMap<DeploymentId, Ptr<CostModelSource>>,
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
    ) -> Result<HashMap<DeploymentId, Ptr<CostModelSource>>, ResolutionError> {
        let url_string = url.to_string();

        let fetched = match self.fetch_cost_model_sources(url, indexings).await {
            Ok(fetched) => fetched.into_iter().map(Ptr::new).collect::<Vec<_>>(),
            Err(err) => {
                tracing::debug!(error=%err, "cost model sources fetch failed");

                // If the data fetch failed, return the cached data
                // If no cached data is available, return the error
                let cached_progress = self.get_from_cache(&url_string, indexings).await;
                return if cached_progress.is_empty() {
                    Err(err)
                } else {
                    Ok(cached_progress)
                };
            }
        };

        let fresh_sources = fetched
            .into_iter()
            .map(|resp| (resp.deployment, resp))
            .collect::<HashMap<_, _>>();

        // Update the cache with the fetched data, if any
        if !fresh_sources.is_empty() {
            self.update_cache(&url_string, &fresh_sources).await;
        }

        // Get the cached data for the missing deployments
        let cached_sources = {
            // Get the list of deployments that are missing from the fetched data
            let missing_indexings = fresh_sources
                .keys()
                .filter(|deployment| !indexings.contains(deployment));

            // Get the cached data for the missing deployments
            self.get_from_cache(&url_string, missing_indexings).await
        };

        // Merge the fetched and cached data
        let fresh_sources = fresh_sources.into_iter();
        let cached_progress = cached_sources.into_iter();
        Ok(HashMap::from_iter(cached_progress.chain(fresh_sources)))
    }

    /// Fetches the cost model sources for the given deployments from the indexer.
    ///
    /// Returns a map of deployment IDs to the retrieved cost model sources. If certain deployment
    /// ID's cost model fetch fails, the corresponding value in the map is `None`.
    pub async fn resolve(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> Result<HashMap<DeploymentId, Ptr<CostModelSource>>, ResolutionError> {
        self.resolve_with_cache(url, indexings).await
    }
}
