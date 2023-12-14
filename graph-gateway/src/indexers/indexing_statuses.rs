use std::borrow::Cow;

use alloy_primitives::BlockHash;
use graphql_http::http_client::ReqwestExt;
use indoc::formatdoc;
use itertools::Itertools;
use serde::{Deserialize, Deserializer};
use thegraph::types::DeploymentId;
use toolshed::url::Url;

pub async fn query(
    client: reqwest::Client,
    status_url: Url,
    deployments: &[DeploymentId],
) -> anyhow::Result<IndexingStatusesResponse> {
    let deployments = deployments.iter().map(|d| format!("\"{d}\"")).join(",");
    let query = formatdoc! {
        r#"{{
            indexingStatuses(subgraphs: [{deployments}]) {{
                subgraph
                chains {{
                    network
                    latestBlock {{
                        number
                        hash
                    }}
                    earliestBlock {{
                        number
                        hash
                    }}
                }}
            }}
        }}"#
    };
    match client.post(status_url.0).send_graphql(query).await {
        Ok(res) => Ok(res?),
        Err(err) => Err(anyhow::anyhow!(
            "Error sending indexing statuses query: {err}"
        )),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexingStatusesResponse {
    pub indexing_statuses: Vec<IndexingStatusResponse>,
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
    #[serde(deserialize_with = "deserialize_bad_hex")]
    pub hash: BlockHash,
}

fn deserialize_bad_hex<'de, D>(deserializer: D) -> Result<BlockHash, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Cow::<str>::deserialize(deserializer)?;
    if s == "0x0" {
        return Ok(BlockHash::ZERO);
    }
    s.parse().map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    mod response {
        use super::*;

        use indoc::indoc;

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
