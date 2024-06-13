//! A resolver for the Proof of Indexing (POI) of indexers.
//!
//! The resolver fetches the public POIs of indexers based on the given POIs metadata. It caches the
//! results of these requests to avoid making the same request multiple times.
//!
//! The cache has a TTL of 20 minutes. Entries are considered expired after this time causing the
//! resolver to fetch the public POIs of the indexer again.

use std::{collections::HashMap, time::Duration};

use alloy_primitives::BlockNumber;
use gateway_common::ttl_hash_map::TtlHashMap;
use parking_lot::RwLock;
use thegraph_core::types::{DeploymentId, ProofOfIndexing};
use url::Url;

use crate::{indexers, indexers::public_poi::Error as PublicPoiFetchError};

/// The default TTL for cache entries is 20 minutes. Entries are considered expired after this time.
pub const DEFAULT_CACHE_TLL: Duration = Duration::from_secs(20 * 60); // 20 minutes

/// The timeout for the indexer indexings' POI resolution.
pub const DEFAULT_INDEXER_INDEXING_POIS_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);

/// The number of Public POI queries in a single request.
// TODO: Change visibility once integration tests are moved
pub const POIS_QUERY_BATCH_SIZE: usize = 10;

/// Error that can occur during POI resolution.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ResolutionError {
    /// Resolution timed out.
    #[error("timeout")]
    Timeout,
}

/// A resolver for the Proof of Indexing (POI) of indexers.
#[allow(clippy::type_complexity)]
pub struct PoiResolver {
    client: reqwest::Client,
    cache: RwLock<TtlHashMap<(String, (DeploymentId, BlockNumber)), ProofOfIndexing>>,
    timeout: Duration,
}

impl PoiResolver {
    /// Create a new [`PoiResolver`] with the given client.
    ///
    /// The client is used to make requests to indexers. The resolver caches the results of these
    /// requests to avoid making the same request multiple times.
    ///
    /// By default, the cache has a TTL of 20 minutes, [`DEFAULT_CACHE_TLL`]. Entries are considered
    /// expired after this time causing the resolver to make a new requests to the indexer.
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            cache: RwLock::new(TtlHashMap::with_ttl(DEFAULT_CACHE_TLL)),
            timeout: DEFAULT_INDEXER_INDEXING_POIS_RESOLUTION_TIMEOUT,
        }
    }

    /// Create a new [`PoiResolver`] with the given client and timeout.
    pub fn with_timeout(client: reqwest::Client, timeout: Duration) -> Self {
        Self {
            client,
            cache: RwLock::new(TtlHashMap::with_ttl(DEFAULT_CACHE_TLL)),
            timeout,
        }
    }

    /// Fetch the public POIs of the indexer based on the given POIs metadata.
    async fn fetch_indexer_public_pois(
        &self,
        status_url: &Url,
        pois: &[(DeploymentId, BlockNumber)],
    ) -> Result<
        HashMap<(DeploymentId, BlockNumber), Result<ProofOfIndexing, PublicPoiFetchError>>,
        ResolutionError,
    > {
        tokio::time::timeout(
            self.timeout,
            send_requests(&self.client, status_url, pois, POIS_QUERY_BATCH_SIZE),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)
    }

    /// Gets the cached Public POIs information for the given deployment-block number pairs.
    ///
    /// This method locks the cache in read mode and returns the cached information.
    fn get_from_cache<'a>(
        &self,
        url: &str,
        keys: impl IntoIterator<Item = &'a (DeploymentId, BlockNumber)>,
    ) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
        let cache_read = self.cache.read();
        let mut result = HashMap::new();

        for key in keys {
            match cache_read.get(&(url.to_owned(), *key)) {
                Some(value) => {
                    result.insert(*key, *value);
                }
                None => continue,
            }
        }

        result
    }

    /// Updates the cache with the given Public POIs information.
    ///
    /// This method locks the cache in write mode and updates the cache with the given progress
    /// information.
    fn update_cache<'a>(
        &self,
        url: &str,
        data: impl IntoIterator<Item = (&'a (DeploymentId, BlockNumber), &'a ProofOfIndexing)>,
    ) {
        let mut cache_write = self.cache.write();
        for (key, value) in data {
            cache_write.insert((url.to_owned(), *key), *value);
        }
    }

    /// Resolves the Public POIs of the indexer based on the given POIs metadata.
    ///
    /// If the request successfully returns the data, the cached data is updated and the new data is
    /// returned, otherwise the cached data is returned.
    async fn resolve_with_cache(
        &self,
        url: &Url,
        poi_requests: &[(DeploymentId, BlockNumber)],
    ) -> Result<HashMap<(DeploymentId, BlockNumber), ProofOfIndexing>, ResolutionError> {
        let status_url = indexers::status_url(url);
        let status_url_string = status_url.to_string();

        let fetched = match self
            .fetch_indexer_public_pois(&status_url, poi_requests)
            .await
        {
            Ok(fetched) => fetched,
            Err(err) => {
                tracing::debug!(error=%err, "indexer public pois fetch failed");

                // If the data fetch failed, return the cached data
                // If no cached data is available, return the error
                let cached_info = self.get_from_cache(&status_url_string, poi_requests);
                return if cached_info.is_empty() {
                    Err(err)
                } else {
                    Ok(cached_info)
                };
            }
        };

        let fresh_info = fetched
            .into_iter()
            .filter_map(|(meta, result)| {
                // TODO: Report the errors instead of filtering them out
                Some((meta, result.ok()?))
            })
            .collect::<HashMap<_, _>>();

        // Update the cache with the fetched data, if any
        if !fresh_info.is_empty() {
            self.update_cache(&status_url_string, &fresh_info);
        }

        // Get the cached data for the missing deployments
        let cached_info = {
            // Get the list of deployments that are missing from the fetched data
            let missing_indexings = fresh_info
                .keys()
                .filter(|meta| !poi_requests.contains(meta));

            // Get the cached data for the missing deployments
            self.get_from_cache(&status_url_string, missing_indexings)
        };

        // Merge the fetched and cached data
        Ok(cached_info.into_iter().chain(fresh_info).collect())
    }

    /// Resolve the public POIs of the indexer based on the given POIs metadata.
    ///
    /// If the public POIs of the indexer are already in the cache, the resolver returns them.
    pub async fn resolve(
        &self,
        url: &Url,
        poi_requests: &[(DeploymentId, BlockNumber)],
    ) -> Result<HashMap<(DeploymentId, BlockNumber), ProofOfIndexing>, ResolutionError> {
        self.resolve_with_cache(url, poi_requests).await
    }
}

/// Send requests to the indexer to get the Public POIs of the given deployment-block number pairs.
///
/// Given a list of deployment-block number pairs, the function sends requests to the indexer to get
/// the Public POIs of the indexers. The function batches the queries into groups of `batch_size`
/// and sends them in a single request. All requests are sent concurrently to the indexer. The
/// function returns a map of deployment-block number pairs to the Public POIs of the indexers, or
/// an error if the request failed.
// TODO: Change visibility once integration tests are moved
pub async fn send_requests(
    client: &reqwest::Client,
    status_url: &Url,
    poi_requests: &[(DeploymentId, BlockNumber)],
    batch_size: usize,
) -> HashMap<(DeploymentId, BlockNumber), Result<ProofOfIndexing, PublicPoiFetchError>> {
    // Batch the POI queries into groups of `batch_size`
    let batches = poi_requests.chunks(batch_size);

    // Create a request for each batch
    let requests = batches.map(|batch| {
        let status_url = status_url.clone();
        async move {
            // Request the indexings' POIs
            let response = indexers::public_poi::send_request(client, status_url, batch).await;

            let result = match response {
                Err(err) => {
                    // If the request failed, mark all deployment-block number pairs in the batch as
                    // failed.
                    return batch
                        .iter()
                        .map(|meta| (*meta, Err(err.clone())))
                        .collect::<HashMap<_, _>>();
                }
                Ok(res) => res,
            };

            // Construct a map of deployment IDs to responses
            result
                .into_iter()
                .filter_map(|res| {
                    Some((
                        (res.deployment, res.block.number),
                        Ok(res.proof_of_indexing?),
                    ))
                })
                .collect::<HashMap<_, _>>()
        }
    });

    // Send all queries concurrently
    let responses = futures::future::join_all(requests).await;

    // Merge all responses into a single map
    responses.into_iter().flatten().collect()
}
