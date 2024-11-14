//! Resolves the cost models for the indexers' deployments.
//!
//! The cost models are fetched from the indexer's cost URL.

use std::{collections::HashMap, time::Duration};

use thegraph_core::DeploymentId;
use url::Url;

use crate::{indexers, indexers::cost_models::CostModelSource};

/// Resolve the indexers' cost models sources and compile them into cost models.
pub struct CostModelResolver {
    client: reqwest::Client,
    timeout: Duration,
    cache: parking_lot::Mutex<HashMap<DeploymentId, u128>>,
}

impl CostModelResolver {
    pub fn new(client: reqwest::Client, timeout: Duration) -> Self {
        Self {
            client,
            timeout,
            cache: Default::default(),
        }
    }

    async fn fetch_cost_model_sources(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> anyhow::Result<Vec<CostModelSource>> {
        let indexer_cost_url = indexers::cost_url(url);
        tokio::time::timeout(
            self.timeout,
            indexers::cost_models::send_request(&self.client, indexer_cost_url, indexings),
        )
        .await?
        .map_err(Into::into)
    }

    /// Fetches the cost model sources for the given deployments from the indexer.
    ///
    /// Returns a map of deployment IDs to the retrieved cost model sources. If certain deployment
    /// ID's cost model fetch fails, the corresponding value in the map is `None`.
    pub async fn resolve(
        &self,
        url: &Url,
        indexings: &[DeploymentId],
    ) -> HashMap<DeploymentId, u128> {
        let sources = match self.fetch_cost_model_sources(url, indexings).await {
            Ok(sources) => sources,
            Err(cost_model_err) => {
                tracing::debug!(%url, %cost_model_err);
                return self.cache.lock().clone();
            }
        };

        // Only support cost models of the form `default => x;`.
        let cost_models: HashMap<DeploymentId, u128> = sources
            .into_iter()
            .filter_map(|src| Some((src.deployment, parse_simple_cost_model(&src.model)?)))
            .collect();

        *self.cache.lock() = cost_models.clone();
        cost_models
    }
}

fn parse_simple_cost_model(src: &str) -> Option<u128> {
    let (_, rest) = src.split_once("default")?;
    let (_, rest) = rest.split_once("=>")?;
    let (consumed, _) = rest.split_once(";")?;
    let token = consumed.trim();
    let fee: f64 = token.parse().ok()?;
    Some((fee * 1e18) as u128)
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_simple_cost_model() {
        let tests = [
            ("default => 0;", 0),
            ("default => 1;", 1000000000000000000),
            ("default => 0.00001;", 10000000000000),
            ("  default  =>  0.004100  ; ", 4100000000000000),
        ];
        for (src, expected) in tests {
            assert_eq!(super::parse_simple_cost_model(src), Some(expected));
        }
    }
}
