//! A resolver that fetches the indexing progress of deployments from an indexer's status URL.

use std::{collections::HashMap, time::Duration};

use alloy_primitives::BlockNumber;
use gateway_common::{caching::Freshness, ttl_hash_map::TtlHashMap};
use parking_lot::RwLock;
use thegraph_core::types::DeploymentId;
use url::Url;

use crate::{
    indexers,
    indexers::indexing_progress::{ChainStatus, Error as IndexingProgressFetchError},
};

/// The default timeout for the indexer's indexing progress resolution.
pub const DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(25);

/// The default TTL (time-to-live) for the cached indexer's indexing progress information: 30 minutes.
///
/// The cache TTL is the time that the indexer's indexing progress resolution is cached for.
pub const DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_CACHE_TTL: Duration =
    Duration::from_secs(60 * 30);

/// The number of deployments indexing progress to query in a single request.
const INDEXINGS_PER_REQUEST_BATCH_SIZE: usize = 100;

/// An error that occurred while resolving the indexer's progress.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ResolutionError {
    /// An error occurred while fetching the indexer progress.
    ///
    /// This includes network errors, timeouts, and deserialization errors.
    #[error("fetch error: {0}")]
    FetchError(#[from] IndexingProgressFetchError),

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
    ) -> HashMap<DeploymentId, Result<Vec<ChainStatus>, ResolutionError>> {
        let status_url = indexers::status_url(url);
        let res = tokio::time::timeout(
            self.timeout,
            send_requests(
                &self.client,
                status_url,
                indexings,
                INDEXINGS_PER_REQUEST_BATCH_SIZE,
            ),
        )
        .await;

        match res {
            Ok(res) => res
                .into_iter()
                .map(|(deployment_id, result)| (deployment_id, result.map_err(Into::into)))
                .collect(),
            // If the request timed out, return a timeout error for all deployments
            Err(_) => indexings
                .iter()
                .map(|deployment_id| (*deployment_id, Err(ResolutionError::Timeout)))
                .collect(),
        }
    }

    /// Gets the cached progress information for the given indexings.
    ///
    /// This method locks the cache in read mode and returns the cached progress information for the
    /// given indexings.
    fn get_from_cache<'a>(
        &self,
        url: &str,
        keys: impl IntoIterator<Item = &'a DeploymentId>,
    ) -> HashMap<DeploymentId, IndexingProgressInfo> {
        let read_cache = self.cache.read();
        let mut result = HashMap::new();

        for key in keys {
            match read_cache.get(&(url.to_owned(), *key)) {
                Some(data) => {
                    result.insert(*key, data.clone());
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
    fn update_cache<'a>(
        &self,
        url: &str,
        data: impl IntoIterator<Item = (&'a DeploymentId, &'a IndexingProgressInfo)>,
    ) {
        let mut write_cache = self.cache.write();
        for (key, value) in data {
            write_cache.insert((url.to_owned(), *key), value.to_owned());
        }
    }

    /// Resolves the indexing progress of the given deployments.
    ///
    /// The function fetches the indexing progress of the given deployments from the indexer's
    /// status URL. If the fetch fails, the function returns the cached data for the failed
    /// deployments. If the fetch succeeds, the function updates the cache with the fetched data.
    ///
    /// Returns a map of deployment IDs to their indexing progress information.
    pub async fn resolve(
        &self,
        url: &Url,
        indexer_deployments: &[DeploymentId],
    ) -> HashMap<DeploymentId, Freshness<IndexingProgressInfo>> {
        let url_string = url.to_string();

        // Fetch the indexings' indexing progress
        let fetched = self.fetch_indexing_progress(url, indexer_deployments).await;

        // Filter out the failures
        let fresh_data = fetched
            .into_iter()
            .filter_map(|(deployment_id, result)| {
                // TODO: Report the errors instead of filtering them out
                Some((deployment_id, result.ok()?))
            })
            .filter_map(|(deployment_id, chains)| {
                // Only consider the first chain status
                let chain = chains.first()?;

                // If the status has no chains or no latest block, skip it
                let info_chain = chain.network.to_owned();
                let info_latest_block = chain.latest_block.as_ref().map(|block| block.number)?;
                let info_min_block = chain.earliest_block.as_ref().map(|block| block.number);

                Some((
                    deployment_id,
                    IndexingProgressInfo {
                        chain: info_chain,
                        latest_block: info_latest_block,
                        min_block: info_min_block,
                    },
                ))
            })
            .collect::<HashMap<_, _>>();

        // Update the cache with the fetched data, if any
        if !fresh_data.is_empty() {
            self.update_cache(&url_string, &fresh_data);
        }

        // Get the cached data for the missing deployments
        let cached_data = {
            // Get the list of deployments that are missing from the fetched data
            let missing_indexings = fresh_data
                .keys()
                .filter(|deployment| !indexer_deployments.contains(deployment));

            // Get the cached data for the missing deployments
            self.get_from_cache(&url_string, missing_indexings)
        };

        // Merge the fetched and cached data
        let fresh_data = fresh_data
            .into_iter()
            .map(|(k, v)| (k, Freshness::Fresh(v)));
        let cached_data = cached_data
            .into_iter()
            .map(|(k, v)| (k, Freshness::Cached(v)));
        HashMap::from_iter(cached_data.chain(fresh_data))
    }
}

/// Sends requests to the indexer's status URL to fetch the indexing progress of deployments.
///
/// Given a list of deployment IDs, the function groups them into batches of a given size and sends
/// all requests concurrently. If one request fails, the function marks all deployments in the batch
/// as failed. The function returns a map of deployment IDs to the indexing progress information.
async fn send_requests(
    client: &reqwest::Client,
    url: indexers::StatusUrl,
    indexings: &[DeploymentId],
    batch_size: usize,
) -> HashMap<DeploymentId, Result<Vec<ChainStatus>, IndexingProgressFetchError>> {
    // Group the deployments into batches of `batch_size`
    let request_batches = indexings.chunks(batch_size);

    // Create a request for each batch
    let requests = request_batches.map(|batch| {
        let url = url.clone();
        async move {
            // Request the indexing progress
            let response =
                indexers::indexing_progress::send_request(client, url.clone(), batch).await;

            let result = match response {
                Err(err) => {
                    // If the request failed, mark all deployment IDs in the batch as failed
                    return batch
                        .iter()
                        .map(|deployment_id| (*deployment_id, Err(err.clone())))
                        .collect::<HashMap<_, _>>();
                }
                Ok(res) => res,
            };

            // Construct a map of deployment IDs to responses
            result
                .into_iter()
                .filter(|response| {
                    batch.contains(&response.deployment_id) && !response.chains.is_empty()
                })
                .map(|response| (response.deployment_id, Ok(response.chains)))
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
    use super::{send_requests, INDEXINGS_PER_REQUEST_BATCH_SIZE};

    mod it_indexing_progress_resolution {
        use std::time::Duration;

        use assert_matches::assert_matches;
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

            let test_deployments = [
                parse_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"),
                parse_deployment_id("QmSqxfDGyGenGFPkqw9sqnYar4XgzaioVWNvhw5QQ3RB1U"),
            ];

            //* When
            let indexing_statuses = tokio::time::timeout(
                Duration::from_secs(60),
                send_requests(
                    &client,
                    status_url,
                    &test_deployments,
                    INDEXINGS_PER_REQUEST_BATCH_SIZE,
                ),
            )
            .await
            .expect("request timed out");

            //* Then
            assert_eq!(indexing_statuses.len(), 2);

            // Status for the first deployment
            let chain_status1 = indexing_statuses
                .get(&test_deployments[0])
                .expect("missing status for deployment 1")
                .as_ref()
                .expect("fetch failed");

            assert_eq!(chain_status1.len(), 1);
            let chain = &chain_status1[0];
            assert_eq!(chain.network, "mainnet");
            assert_matches!(chain.latest_block, Some(ref block) => {
                assert!(block.number > 0);
            });
            assert_matches!(chain.earliest_block, Some(ref block) => {
                assert!(block.number > 0);
            });

            // Status for the second deployment
            let chain_status2 = indexing_statuses
                .get(&test_deployments[1])
                .expect("missing status for deployment")
                .as_ref()
                .expect("fetch failed");

            assert_eq!(chain_status2.len(), 1);
            let chain = &chain_status2[0];
            assert_eq!(chain.network, "mainnet");
            assert_matches!(chain.latest_block, Some(ref block) => {
                assert!(block.number > 0);
            });
            assert_matches!(chain.earliest_block, Some(ref block) => {
                assert!(block.number > 0);
            });
        }
    }
}
