//! A resolver for the Proof of Indexing (POI) of indexers.
//!
//! The resolver fetches the public POIs of indexers based on the given POIs metadata. It caches the
//! results of these requests to avoid making the same request multiple times.
//!
//! The cache has a TTL of 20 minutes. Entries are considered expired after this time causing the
//! resolver to fetch the public POIs of the indexer again.

use std::{collections::HashMap, time::Duration};

use parking_lot::RwLock;
use thegraph_core::{alloy::primitives::BlockNumber, DeploymentId, ProofOfIndexing};
use url::Url;

use crate::{
    indexers, indexers::public_poi::Error as PublicPoiFetchError, ttl_hash_map::TtlHashMap,
};

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

/// A resolver for the Proof of Indexing (POI) of indexers. Results are cached for some TTL to avoid
/// making the same request multiple times.
#[allow(clippy::type_complexity)]
pub struct PoiResolver {
    client: reqwest::Client,
    cache: RwLock<TtlHashMap<(String, (DeploymentId, BlockNumber)), ProofOfIndexing>>,
    timeout: Duration,
}

impl PoiResolver {
    /// Create a new [`PoiResolver`] with the given timeout and cache TTL.
    pub fn new(client: reqwest::Client, timeout: Duration, cache_ttl: Duration) -> Self {
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
        let status_url = url.join("status").unwrap();
        let res = tokio::time::timeout(
            self.timeout,
            send_requests(&self.client, &status_url, pois, POIS_PER_REQUEST_BATCH_SIZE),
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
        let mut results = self.get_from_cache(&url_string, poi_requests);
        let missing_requests: Vec<(DeploymentId, BlockNumber)> = poi_requests
            .iter()
            .filter(|r| !results.contains_key(r))
            .cloned()
            .collect();
        if missing_requests.is_empty() {
            return results;
        }

        let fetched: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> = self
            .fetch_indexer_public_pois(url, &missing_requests)
            .await
            .into_iter()
            .filter_map(|(key, result)| match result {
                Ok(poi) => Some((key, poi)),
                Err(poi_fetch_err) => {
                    tracing::warn!(%poi_fetch_err, ?key);
                    None
                }
            })
            .collect();
        self.update_cache(&url_string, &fetched);
        results.extend(fetched);
        results
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
    status_url: &Url,
    poi_requests: &[(DeploymentId, BlockNumber)],
    batch_size: usize,
) -> HashMap<(DeploymentId, BlockNumber), Result<ProofOfIndexing, PublicPoiFetchError>> {
    // Batch the POI queries into groups of `batch_size`
    let request_batches = poi_requests.chunks(batch_size);

    // Create a request for each batch
    let requests = request_batches.map(|batch| {
        let status_url = status_url.clone();
        async move {
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

    // Send all requests concurrently
    let responses = futures::future::join_all(requests).await;

    // Merge the responses into a single map
    responses.into_iter().flatten().collect()
}
