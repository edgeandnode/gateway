//! A resolver that fetches the indexing progress of deployments from an indexer's status URL.

use std::{collections::HashMap, time::Duration};

use alloy_primitives::BlockNumber;
use thegraph_core::types::DeploymentId;
use url::Url;

use crate::{indexers, indexers::indexing_statuses::IndexingStatusResponse};

/// The timeout for the indexer's indexing progress resolution.
pub const DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);

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
#[derive(Debug)]
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
}

impl IndexingProgressResolver {
    /// Creates a new [`IndexingProgressResolver`].
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            timeout: DEFAULT_INDEXER_INDEXING_PROGRESS_RESOLUTION_TIMEOUT,
        }
    }

    /// Creates a new [`IndexingProgressResolver`] with the given timeout.
    pub fn with_timeout(client: reqwest::Client, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    /// Resolves the indexer indexing progress for the given deployments
    async fn resolve_indexing_progress(
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

    /// Resolves the indexing progress of the given deployments.
    ///
    /// Returns a map of deployment IDs to their indexing progress information.
    pub async fn resolve(
        &self,
        url: &Url,
        indexer_deployments: &[DeploymentId],
    ) -> Result<HashMap<DeploymentId, IndexingProgressInfo>, ResolutionError> {
        let progress = self
            .resolve_indexing_progress(url, indexer_deployments)
            .await?;

        let progress = progress
            .into_iter()
            .filter_map(|resp| {
                // TODO: Review this
                // Only consider the first chain status, if has no chains
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

        Ok(progress)
    }
}
