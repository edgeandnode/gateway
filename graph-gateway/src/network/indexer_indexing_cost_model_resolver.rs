//! Resolves the cost models for the indexers' deployments.
//!
//! The cost models are fetched from the indexer's cost URL.

use std::{collections::HashMap, time::Duration};

use thegraph_core::types::DeploymentId;
use url::Url;

use crate::{indexers, indexers::cost_models::CostModelSource};

/// The default timeout for the indexer indexings' cost model resolution.
pub const DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Error that can occur during cost model resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {
    /// Cost model fetch failed.
    #[error("fetch error: {0}")]
    FetchError(anyhow::Error),

    /// Resolution timed out.
    #[error("timeout")]
    Timeout,
}

/// Resolve the indexers' cost models sources and compile them into cost models.
pub struct CostModelResolver {
    client: reqwest::Client,
    timeout: Duration,
}

impl CostModelResolver {
    /// Creates a new [`CostModelResolver`] with the given HTTP client.
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            timeout: DEFAULT_INDEXER_INDEXING_COST_MODEL_RESOLUTION_TIMEOUT,
        }
    }

    /// Creates a new [`CostModelResolver`] with the given HTTP client and timeout.
    pub fn with_timeout(client: reqwest::Client, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    async fn resolve_cost_model(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> Result<Vec<CostModelSource>, ResolutionError> {
        let indexer_cost_url = indexers::cost_url(url);
        tokio::time::timeout(
            self.timeout,
            // TODO: Handle the different errors once the indexers client module reports them
            indexers::cost_models::query(&self.client, indexer_cost_url, indexings),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(ResolutionError::FetchError)
    }

    /// Fetches the cost model sources for the given deployments from the indexer.
    ///
    /// Returns a map of deployment IDs to the retrieved cost model sources. If certain deployment
    /// ID's cost model fetch fails, the corresponding value in the map is `None`.
    pub async fn resolve(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> anyhow::Result<HashMap<DeploymentId, CostModelSource>> {
        let sources = self
            .resolve_cost_model(url, indexings)
            .await?
            .into_iter()
            .map(|model| {
                let deployment_id = model.deployment;
                (deployment_id, model)
            })
            .collect::<HashMap<_, _>>();

        Ok(sources)
    }
}
