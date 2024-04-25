//! Resolves the cost models for the indexers' deployments.
//!
//! The cost models are fetched from the indexer's cost URL.

use std::collections::HashMap;

use thegraph_core::types::DeploymentId;
use url::Url;

use crate::{indexers, indexers::cost_models::CostModelSource};

/// Error type for cost model resolution.
#[derive(Debug, Clone, thiserror::Error)]
enum ResolutionError {
    /// The cost model couldn't be fetched.
    #[error("The cost model couldn't be fetched: {0}")]
    FetchFailed(String),
}

/// Resolve the indexers' cost models sources and compile them into cost models.
pub struct CostModelResolver {
    client: reqwest::Client,
}

impl CostModelResolver {
    /// Creates a new [`CostModelResolver`] with the given HTTP client.
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Fetches the cost model sources for the given deployments from the indexer.
    ///
    /// Returns a map of deployment IDs to the retrieved cost model sources. If certain deployment
    /// ID's cost model fetch fails, the corresponding value in the map is `None`.
    pub async fn resolve(
        &self,
        indexer_cost_url: Url,
        indexer_deployments: &[DeploymentId],
    ) -> HashMap<DeploymentId, CostModelSource> {
        let sources =
            match indexers::cost_models::query(&self.client, indexer_cost_url, indexer_deployments)
                .await
                .map_err(|err| ResolutionError::FetchFailed(err.to_string()))
            {
                Ok(sources) => sources,
                Err(err) => {
                    tracing::debug!("Failed to fetch cost models: {err}");
                    return HashMap::new();
                }
            };

        sources
            .into_iter()
            .map(|model| {
                let deployment_id = model.deployment;
                (deployment_id, model)
            })
            .collect::<HashMap<_, _>>()
    }
}
