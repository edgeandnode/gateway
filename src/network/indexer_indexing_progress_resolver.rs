//! A resolver that fetches the indexing progress of deployments from an indexer's status URL.

use std::{collections::HashMap, time::Duration};

use parking_lot::{Mutex, RwLock};
use thegraph_core::{BlockNumber, DeploymentId};
use url::Url;

use crate::{
    indexers,
    indexers::indexing_progress::{ChainStatus, Error as IndexingProgressFetchError},
};

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
    /// The latest block number indexed by the indexer.
    pub latest_block: BlockNumber,
    /// The earliest block number indexed by the indexer.
    pub min_block: Option<BlockNumber>,
}

/// A resolver that fetches the indexing progress of deployments from an indexer's status URL.
pub struct IndexingProgressResolver {
    client: reqwest::Client,
    timeout: Duration,
    cache: RwLock<HashMap<String, Mutex<HashMap<DeploymentId, IndexingProgressInfo>>>>,
}

impl IndexingProgressResolver {
    pub fn new(client: reqwest::Client, timeout: Duration) -> Self {
        Self {
            client,
            timeout,
            cache: Default::default(),
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
    ) -> HashMap<DeploymentId, IndexingProgressInfo> {
        let url_string = url.to_string();
        let results = self.fetch_indexing_progress(url, indexer_deployments).await;

        let mut outer_cache = self.cache.read();
        if !outer_cache.contains_key(&url_string) {
            drop(outer_cache);
            self.cache
                .write()
                .insert(url_string.clone(), Default::default());
            outer_cache = self.cache.read();
        }

        let mut cache = outer_cache.get(&url_string).unwrap().lock();
        for (deployment, result) in results {
            let status = result.ok().and_then(|chains| {
                let chain = chains.first()?;
                Some(IndexingProgressInfo {
                    latest_block: chain.latest_block.as_ref().map(|block| block.number)?,
                    min_block: chain.earliest_block.as_ref().map(|block| block.number),
                })
            });
            if let Some(status) = status {
                cache.insert(deployment, status);
            }
        }
        cache.clone()
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
