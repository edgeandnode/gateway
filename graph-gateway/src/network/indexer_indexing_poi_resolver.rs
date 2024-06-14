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
const POIS_PER_REQUEST_BATCH_SIZE: usize = 10;

/// Error that can occur during POI resolution.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ResolutionError {
    /// An error occurred while fetching the Public POIs of the indexer.
    ///
    /// This includes network errors, timeouts, and deserialization errors.
    #[error("fetch error: {0}")]
    FetchError(#[from] PublicPoiFetchError),

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
            timeout: DEFAULT_INDEXER_INDEXING_POIS_RESOLUTION_TIMEOUT,
            cache: RwLock::new(TtlHashMap::with_ttl(DEFAULT_CACHE_TLL)),
        }
    }

    /// Create a new [`PoiResolver`] with the given timeout and cache TTL.
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

    /// Fetch the public POIs of the indexer based on the given POIs metadata.
    async fn fetch_indexer_public_pois(
        &self,
        url: &Url,
        pois: &[(DeploymentId, BlockNumber)],
    ) -> HashMap<(DeploymentId, BlockNumber), Result<ProofOfIndexing, ResolutionError>> {
        let status_url = indexers::status_url(url);
        let res = tokio::time::timeout(
            self.timeout,
            send_requests(&self.client, status_url, pois, POIS_PER_REQUEST_BATCH_SIZE),
        )
        .await;

        match res {
            Ok(res) => res
                .into_iter()
                .map(|(meta, result)| (meta, result.map_err(Into::into)))
                .collect(),
            // If the request timed out, return a timeout error for all deployment-block number pairs
            Err(_) => pois
                .iter()
                .map(|meta| (*meta, Err(ResolutionError::Timeout)))
                .collect(),
        }
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

    /// Resolve the public POIs of the indexer based on the given POIs metadata.
    ///
    /// If the public POIs of the indexer are already in the cache, the resolver returns them.
    pub async fn resolve(
        &self,
        url: &Url,
        poi_requests: &[(DeploymentId, BlockNumber)],
    ) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
        let url_string = url.to_string();

        // Fetch the indexings' indexing progress
        let fetched = self.fetch_indexer_public_pois(url, poi_requests).await;

        // Filter out the failures
        let fresh_data = fetched
            .into_iter()
            .filter_map(|(meta, result)| {
                // TODO: Report the errors instead of filtering them out
                Some((meta, result.ok()?))
            })
            .collect::<HashMap<_, _>>();

        // Update the cache with the fetched data, if any
        if !fresh_data.is_empty() {
            self.update_cache(&url_string, &fresh_data);
        }

        // Get the cached data for the missing deployments
        let cached_info = {
            // Get the list of deployments that are missing from the fetched data
            let missing_indexings = fresh_data
                .keys()
                .filter(|meta| !poi_requests.contains(meta));

            // Get the cached data for the missing deployments
            self.get_from_cache(&url_string, missing_indexings)
        };

        // Merge the fetched and cached data
        cached_info.into_iter().chain(fresh_data).collect()
    }
}

/// Send requests to the indexer to get the Public POIs of the given deployment-block number pairs.
///
/// Given a list of deployment-block number pairs, the function sends requests to the indexer to get
/// the Public POIs of the indexers. The function batches the queries into groups of `batch_size`
/// and sends them in a single request. All requests are sent concurrently to the indexer. The
/// function returns a map of deployment-block number pairs to the Public POIs of the indexers, or
/// an error if the request failed.
async fn send_requests(
    client: &reqwest::Client,
    url: indexers::StatusUrl,
    poi_requests: &[(DeploymentId, BlockNumber)],
    batch_size: usize,
) -> HashMap<(DeploymentId, BlockNumber), Result<ProofOfIndexing, PublicPoiFetchError>> {
    // Batch the POI queries into groups of `batch_size`
    let request_batches = poi_requests.chunks(batch_size);

    // Create a request for each batch
    let requests = request_batches.map(|batch| {
        let url = url.clone();
        async move {
            // Request the indexings' POIs
            let response = indexers::public_poi::send_request(client, url.clone(), batch).await;

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

    // Send all requests concurrently
    let responses = futures::future::join_all(requests).await;

    // Merge the responses into a single map
    responses.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::{send_requests, POIS_PER_REQUEST_BATCH_SIZE};

    mod it_public_pois_resolution {
        use std::time::Duration;

        use alloy_primitives::BlockNumber;
        use thegraph_core::types::DeploymentId;

        use super::*;
        use crate::indexers;

        /// Test helper to get the testnet indexer url from the environment.
        fn test_indexer_url() -> reqwest::Url {
            std::env::var("IT_TEST_TESTNET_INDEXER_URL")
                .expect("Missing IT_TEST_TESTNET_INDEXER_URL")
                .parse()
                .expect("Invalid IT_TEST_TESTNET_INDEXER_URL")
        }

        /// Parse a deployment ID from a string.
        fn parse_deployment_id(deployment: &str) -> DeploymentId {
            deployment.parse().expect("invalid deployment id")
        }

        #[test_with::env(IT_TEST_TESTNET_INDEXER_URL)]
        #[tokio::test]
        async fn send_batched_queries_and_merge_results() {
            //* Given
            let client = reqwest::Client::new();
            let status_url = indexers::status_url(test_indexer_url());

            let deployment = parse_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");
            let pois_to_query = (1..=POIS_PER_REQUEST_BATCH_SIZE + 2)
                .map(|i| (deployment, i as BlockNumber))
                .collect::<Vec<_>>();

            //* When
            let response = tokio::time::timeout(
                Duration::from_secs(60),
                send_requests(
                    &client,
                    status_url,
                    &pois_to_query,
                    POIS_PER_REQUEST_BATCH_SIZE,
                ),
            )
            .await
            .expect("request timed out");

            //* Then
            assert_eq!(response.len(), POIS_PER_REQUEST_BATCH_SIZE + 2);
            assert!(response.contains_key(&(deployment, 1)));
            assert!(response.contains_key(&(deployment, 2)));
            assert!(response.contains_key(&(deployment, 3)));
        }
    }
}
