use anyhow::{bail, ensure};
use futures::future::join_all;
use graphql_http::http_client::ReqwestExt as _;
use indoc::formatdoc;
use itertools::Itertools;
use serde::Deserialize;
use thegraph::types::DeploymentId;

pub async fn query(
    client: &reqwest::Client,
    status_url: reqwest::Url,
    deployments: &[DeploymentId],
) -> anyhow::Result<Vec<IndexingStatusResponse>> {
    let queries = deployments.chunks(100).map(|deployments| {
        let deployments = deployments.iter().map(|d| format!("\"{d}\"")).join(",");
        let query = formatdoc! {
            r#"{{
                indexingStatuses(subgraphs: [{deployments}]) {{
                    subgraph
                    chains {{
                        network
                        latestBlock {{
                            number
                        }}
                        earliestBlock {{
                            number
                        }}
                    }}
                }}
            }}"#
        };
        client.post(status_url.clone()).send_graphql(query)
    });
    let results: Vec<Result<IndexingStatusesResponse, String>> = join_all(queries)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(err)) => Err(err.to_string()),
            Err(err) => Err(err.to_string()),
        })
        .collect();
    ensure!(!results.is_empty(), "no results");
    if results.iter().all(|r| r.is_err()) {
        let error = results.into_iter().find_map(|r| r.err()).unwrap();
        bail!("{error}");
    }
    let indexing_statuses: Vec<IndexingStatusResponse> = results
        .into_iter()
        .flat_map(|r| r.into_iter().flat_map(|r| r.indexing_statuses))
        .collect();
    Ok(indexing_statuses)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexingStatusesResponse {
    indexing_statuses: Vec<IndexingStatusResponse>,
}

#[derive(Debug, Deserialize)]
pub struct IndexingStatusResponse {
    pub subgraph: DeploymentId,
    pub chains: Vec<ChainStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainStatus {
    pub network: String,
    pub latest_block: Option<BlockStatus>,
    pub earliest_block: Option<BlockStatus>,
}

#[derive(Debug, Deserialize)]
pub struct BlockStatus {
    pub number: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod response {
        use indoc::indoc;

        use super::*;

        #[test]
        fn deserialize_indexing_statuses_response() {
            //// Given
            let response = indoc! {
                r#"{
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
                }"#
            };

            //// When
            let response: IndexingStatusesResponse =
                serde_json::from_str(response).expect("Failed to deserialize response");

            //// Then
            assert_eq!(response.indexing_statuses.len(), 2);

            assert_eq!(response.indexing_statuses[0].chains.len(), 1);
            assert_eq!(response.indexing_statuses[0].chains[0].network, "rinkeby");
            assert!(response.indexing_statuses[0].chains[0]
                .latest_block
                .is_some());
            assert!(response.indexing_statuses[0].chains[0]
                .earliest_block
                .is_some());

            assert_eq!(response.indexing_statuses[1].chains.len(), 1);
            assert_eq!(response.indexing_statuses[1].chains[0].network, "rinkeby");
            assert!(response.indexing_statuses[1].chains[0]
                .latest_block
                .is_none());
            assert!(response.indexing_statuses[1].chains[0]
                .earliest_block
                .is_none());
        }
    }
}
