use std::collections::HashMap;

use anyhow::anyhow;
use thegraph_core::DeploymentId;
use thegraph_graphql_http::http_client::ReqwestExt;
use url::Url;

use crate::network::GraphQlRequest;

pub struct CostModelResolver {
    http: reqwest::Client,
    cache: parking_lot::Mutex<HashMap<DeploymentId, u128>>,
}

impl CostModelResolver {
    pub fn new(http: reqwest::Client) -> Self {
        Self {
            http,
            cache: Default::default(),
        }
    }

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
            .filter_map(|(deployment, src)| Some((deployment, parse_simple_cost_model(&src)?)))
            .collect();

        *self.cache.lock() = cost_models.clone();
        cost_models
    }

    async fn fetch_cost_model_sources(
        &self,
        url: &Url,
        deployments: &[DeploymentId],
    ) -> anyhow::Result<HashMap<DeploymentId, String>> {
        let url = url.join("cost").map_err(|_| anyhow!("invalid URL"))?;

        let query = r#"
            query costModels($deployments: [String!]!) {
                costModels(deployments: $deployments) {
                    deployment
                    model
                }
            }
        "#;
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            cost_models: Vec<CostModelSource>,
        }
        #[derive(serde::Deserialize)]
        pub struct CostModelSource {
            pub deployment: DeploymentId,
            pub model: String,
        }
        let resp = self
            .http
            .post(url)
            .send_graphql::<Response>(GraphQlRequest {
                document: query.to_string(),
                variables: serde_json::json!({ "deployments": deployments }),
            })
            .await??;
        Ok(resp
            .cost_models
            .into_iter()
            .map(|CostModelSource { deployment, model }| (deployment, model))
            .collect())
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
