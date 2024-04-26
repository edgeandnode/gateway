//! A resolver that fetches the indexing statuses of deployments from an indexer's status URL.

use std::collections::HashMap;

use alloy_primitives::BlockNumber;
use thegraph_core::types::DeploymentId;
use url::Url;

use crate::indexers;

/// An error that occurred while resolving the indexing statuses of deployments.
// TODO: Differentiate deserialization errors from network errors.
#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {
    /// An error occurred while querying the indexer status.
    ///
    /// This includes network errors, timeouts, and deserialization errors.
    #[error("failed to query the indexer status: {0}")]
    FetchError(String),
}

/// The indexing status of a deployment on a chain.
#[derive(Debug)]
pub struct IndexingStatusInfo {
    /// The chain the deployment is associated with.
    pub chain: String,
    /// The latest block number indexed by the indexer.
    pub latest_block: BlockNumber,
    /// The earliest block number indexed by the indexer.
    pub min_block: Option<BlockNumber>,
}

/// A resolver that fetches the indexing statuses of deployments from an indexer's status URL.
pub struct IndexingStatusResolver {
    client: reqwest::Client,
}

impl IndexingStatusResolver {
    /// Creates a new [`IndexingStatusResolver`].
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Resolves the indexing statuses of the given deployments.
    ///
    /// The resolver fetches the indexing statuses from the indexer status URL.
    ///
    /// Returns a map of deployment IDs to their indexing statuses.
    pub async fn resolve(
        &self,
        indexer_status_url: Url,
        indexer_deployments: &[DeploymentId],
    ) -> Result<HashMap<DeploymentId, IndexingStatusInfo>, ResolutionError> {
        let statuses = indexers::indexing_statuses::query(
            &self.client,
            indexer_status_url,
            indexer_deployments,
        )
        .await
        .map_err(|e| ResolutionError::FetchError(e.to_string()))?;

        let statuses = statuses
            .into_iter()
            .filter_map(|status| {
                // Only consider the first chain status, if has no chains
                let chain = status.chains.into_iter().next()?;

                // If the status has no chains or no latest block, skip it
                let status_chain = chain.network;
                let status_latest_block = chain.latest_block.map(|block| block.number)?;
                let status_min_block = chain.earliest_block.as_ref().map(|block| block.number);

                Some((
                    status.subgraph,
                    IndexingStatusInfo {
                        chain: status_chain,
                        latest_block: status_latest_block,
                        min_block: status_min_block,
                    },
                ))
            })
            .collect::<HashMap<_, _>>();

        Ok(statuses)
    }
}
