use alloy_primitives::BlockNumber;
use serde::Deserialize;
use serde_with::serde_as;
use thegraph_core::types::DeploymentId;
use thegraph_graphql_http::{
    graphql::{Document, IntoDocument, IntoDocumentWithVariables},
    http_client::{RequestError, ReqwestExt as _, ResponseError},
};

const INDEXING_PROGRESS_QUERY_DOCUMENT: &str = indoc::indoc! {r#"
    query indexingProgress($deployments: [String!]!) {
        indexingStatuses(subgraphs: $deployments) {
            deploymentId: subgraph
            chains {
                network
                latestBlock { number }
                earliestBlock { number }
            }
        }
    }
"#};

/// Errors that can occur while fetching the indexing progress.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// The request failed.
    #[error("request error: {0}")]
    RequestError(String),

    /// Invalid response.
    ///
    /// The response could not be deserialized or is missing required fields.
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// The response did not contain any progress information.
    #[error("empty response")]
    EmptyResponse,
}

/// Send a request to the indexer to get the indexing status of the given deployments.
pub async fn send_request(
    client: &reqwest::Client,
    status_url: reqwest::Url,
    deployments: impl IntoIterator<Item = &DeploymentId>,
) -> Result<Vec<IndexingStatusResponse>, Error> {
    let resp = client
        .post(status_url)
        .send_graphql::<Response>(Request::new(deployments))
        .await
        .map_err(|err| match err {
            RequestError::RequestSerializationError(..) => {
                unreachable!("request serialization should not fail")
            }
            RequestError::RequestSendError(..) | RequestError::ResponseRecvError(..) => {
                Error::RequestError(err.to_string())
            }
            RequestError::ResponseDeserializationError { .. } => {
                Error::InvalidResponse(err.to_string())
            }
        })?
        .map_err(|err| match err {
            ResponseError::Failure { .. } => Error::RequestError(err.to_string()),
            ResponseError::Empty => Error::EmptyResponse,
        })?;

    if resp.indexing_statuses.is_empty() {
        return Err(Error::EmptyResponse);
    }

    Ok(resp.indexing_statuses)
}

/// The request type for the indexing progress query.
///
/// This type is used to construct the GraphQL request document and variables for the indexing
/// progress query.
///
/// See [`INDEXING_PROGRESS_QUERY_DOCUMENT`] for the query document.
#[derive(Debug, Clone)]
struct Request {
    document: Document,
    vars_deployments: Vec<String>,
}

impl Request {
    /// Create a new indexing progress query request.
    fn new<'a>(deployments: impl IntoIterator<Item = &'a DeploymentId>) -> Self {
        let deployments = deployments
            .into_iter()
            .map(|item| item.to_string())
            .collect();
        Self {
            document: INDEXING_PROGRESS_QUERY_DOCUMENT.into_document(),
            vars_deployments: deployments,
        }
    }
}

impl IntoDocumentWithVariables for Request {
    type Variables = serde_json::Value;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (
            self.document,
            serde_json::json!({ "deployments": self.vars_deployments }),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    indexing_statuses: Vec<IndexingStatusResponse>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexingStatusResponse {
    pub deployment_id: DeploymentId,
    pub chains: Vec<ChainStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainStatus {
    pub network: String,
    pub latest_block: Option<BlockStatus>,
    pub earliest_block: Option<BlockStatus>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct BlockStatus {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub number: BlockNumber,
}

#[cfg(test)]
mod tests {
    use super::Response;

    #[test]
    fn deserialize_response() {
        //* Given
        let response = serde_json::json!({
            "indexingStatuses": [
                {
                    "deploymentId": "QmZTy9EJHu8rfY9QbEk3z1epmmvh5XHhT2Wqhkfbyt8k9Z",
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
                    "deploymentId": "QmSLQfPFcz2pKRJZUH16Sk26EFpRgdxTYGnMiKvWgKRM2a",
                    "chains": [
                        {
                            "network": "rinkeby"
                        }
                    ]
                }
            ]
        });

        //* When
        let response = serde_json::from_value(response);

        //* Then
        let response: Response = response.expect("deserialization failed");

        assert_eq!(response.indexing_statuses.len(), 2);
        let status1 = &response.indexing_statuses[0];
        let status2 = &response.indexing_statuses[1];

        // Status 1
        assert_eq!(
            status1.deployment_id.to_string(),
            "QmZTy9EJHu8rfY9QbEk3z1epmmvh5XHhT2Wqhkfbyt8k9Z"
        );
        assert_eq!(status1.chains.len(), 1);
        assert_eq!(status1.chains[0].network, "rinkeby");
        assert!(status1.chains[0].latest_block.is_some());
        assert!(status1.chains[0].earliest_block.is_some());

        // Status 2
        assert_eq!(
            status2.deployment_id.to_string(),
            "QmSLQfPFcz2pKRJZUH16Sk26EFpRgdxTYGnMiKvWgKRM2a"
        );
        assert_eq!(status2.chains.len(), 1);
        assert_eq!(status2.chains[0].network, "rinkeby");
        assert!(status2.chains[0].latest_block.is_none());
        assert!(status2.chains[0].earliest_block.is_none());
    }
}
