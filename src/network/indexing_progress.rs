use std::collections::HashMap;

use parking_lot::{Mutex, RwLock};
use serde_with::serde_as;
use thegraph_core::{alloy::primitives::BlockNumber, DeploymentId};
use thegraph_graphql_http::http_client::ReqwestExt;
use url::Url;

use super::GraphQlRequest;

#[derive(Debug, Clone)]
pub struct IndexingProgressInfo {
    pub latest_block: BlockNumber,
    pub min_block: Option<BlockNumber>,
}

pub struct IndexingProgressResolver {
    http: reqwest::Client,
    cache: RwLock<HashMap<String, Mutex<HashMap<DeploymentId, IndexingProgressInfo>>>>,
}

impl IndexingProgressResolver {
    pub fn new(http: reqwest::Client) -> Self {
        Self {
            http,
            cache: Default::default(),
        }
    }

    pub async fn resolve(
        &self,
        status_url: &Url,
        deployments: &[DeploymentId],
    ) -> HashMap<DeploymentId, IndexingProgressInfo> {
        let url_string = status_url.to_string();

        let results = send_requests(&self.http, status_url, deployments).await;

        let mut outer_cache = self.cache.read();
        if !outer_cache.contains_key(&url_string) {
            drop(outer_cache);
            self.cache
                .write()
                .insert(url_string.clone(), Default::default());
            outer_cache = self.cache.read();
        }

        let mut cache = outer_cache.get(&url_string).unwrap().lock();
        for (deployment, status) in results {
            cache.insert(deployment, status);
        }
        cache.clone()
    }
}

async fn send_requests(
    http: &reqwest::Client,
    status_url: &Url,
    indexings: &[DeploymentId],
) -> HashMap<DeploymentId, IndexingProgressInfo> {
    let request_batches = indexings.chunks(100);
    let requests = request_batches.map(|batch| {
        let status_url = status_url.clone();
        async move {
            let result = send_request(http, status_url.clone(), batch).await;
            match result {
                Ok(response) => response,
                Err(indexing_progress_err) => {
                    tracing::warn!(%status_url, %indexing_progress_err);
                    Default::default()
                }
            }
        }
    });

    let responses = futures::future::join_all(requests).await;
    responses.into_iter().flatten().collect()
}

async fn send_request(
    client: &reqwest::Client,
    status_url: Url,
    deployments: &[DeploymentId],
) -> anyhow::Result<HashMap<DeploymentId, IndexingProgressInfo>> {
    let query = r#"
        query indexingProgress($deployments: [String!]!) {
            indexingStatuses(subgraphs: $deployments) {
                subgraph
                chains {
                    network
                    latestBlock { number }
                    earliestBlock { number }
                }
            }
        }"#;
    let response = client
        .post(status_url)
        .send_graphql::<Response>(GraphQlRequest {
            document: query.to_string(),
            variables: serde_json::json!({ "deployments": deployments }),
        })
        .await??;
    let statuses = response
        .indexing_statuses
        .into_iter()
        .filter(|info| info.chains.len() == 1)
        .filter_map(|info| {
            let chain = &info.chains[0];
            let status = IndexingProgressInfo {
                latest_block: chain.latest_block.as_ref().map(|block| block.number)?,
                min_block: chain.earliest_block.as_ref().map(|block| block.number),
            };
            Some((info.deployment_id, status))
        })
        .collect();
    Ok(statuses)
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    indexing_statuses: Vec<IndexingStatusResponse>,
}
#[derive(Debug, serde::Deserialize)]
pub struct IndexingStatusResponse {
    #[serde(rename = "subgraph")]
    pub deployment_id: DeploymentId,
    pub chains: Vec<ChainStatus>,
}
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainStatus {
    pub latest_block: Option<BlockStatus>,
    pub earliest_block: Option<BlockStatus>,
}
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct BlockStatus {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub number: BlockNumber,
}

#[cfg(test)]
mod tests {
    use super::Response;

    #[test]
    fn deserialize_response() {
        let response = serde_json::json!({
            "indexingStatuses": [
                {
                    "subgraph": "QmZTy9EJHu8rfY9QbEk3z1epmmvh5XHhT2Wqhkfbyt8k9Z",
                    "chains": [
                        {
                            "network": "rinkeby",
                            "latestBlock": {
                                "number": "10164818",
                                "hash": "0xaa94881130ba16c28cc90a5a880b117bdc90b6b11e9cde0c78804cdb93cc9e85"
                            },
                            "earliestBlock": {
                                "number": "7559999",
                                "hash": "0x0"
                            }
                        }
                    ]
                },
                {
                    "subgraph": "QmSLQfPFcz2pKRJZUH16Sk26EFpRgdxTYGnMiKvWgKRM2a",
                    "chains": [
                        {
                            "network": "rinkeby"
                        }
                    ]
                }
            ]
        });

        let response = serde_json::from_value(response);
        let response: Response = response.expect("deserialization failed");

        assert_eq!(response.indexing_statuses.len(), 2);
        let status1 = &response.indexing_statuses[0];
        let status2 = &response.indexing_statuses[1];

        assert_eq!(
            status1.deployment_id.to_string(),
            "QmZTy9EJHu8rfY9QbEk3z1epmmvh5XHhT2Wqhkfbyt8k9Z"
        );
        assert_eq!(status1.chains.len(), 1);
        assert!(status1.chains[0].latest_block.is_some());
        assert!(status1.chains[0].earliest_block.is_some());

        assert_eq!(
            status2.deployment_id.to_string(),
            "QmSLQfPFcz2pKRJZUH16Sk26EFpRgdxTYGnMiKvWgKRM2a"
        );
        assert_eq!(status2.chains.len(), 1);
        assert!(status2.chains[0].latest_block.is_none());
        assert!(status2.chains[0].earliest_block.is_none());
    }
}
