use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thegraph_core::{BlockNumber, DeploymentId, ProofOfIndexing};
use thegraph_graphql_http::{
    graphql::{Document, IntoDocument, IntoDocumentWithVariables},
    http_client::{RequestError, ReqwestExt, ResponseError},
};

use super::urls::StatusUrl;

const PUBLIC_PROOF_OF_INDEXING_QUERY_DOCUMENT: &str = r#"
    query publicPois($requests: [PublicProofOfIndexingRequest!]!) {
        publicProofsOfIndexing(requests: $requests) {
            deployment
            proofOfIndexing
            block { number }
        }
    }"#;

/// Errors that can occur while fetching the indexer's public POIs.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// The request failed.
    #[error("request error: {0}")]
    Request(String),

    /// Invalid response.
    ///
    /// The response could not be deserialized or is missing required fields.
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// The response did not contain any public POIs.
    #[error("empty response")]
    EmptyResponse,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicProofOfIndexingRequest {
    deployment: DeploymentId,
    block_number: BlockNumber,
}

impl From<(DeploymentId, BlockNumber)> for PublicProofOfIndexingRequest {
    fn from((deployment, block_number): (DeploymentId, BlockNumber)) -> Self {
        Self {
            deployment,
            block_number,
        }
    }
}

/// Send a request to the indexer to get the Public POIs of the given deployment-block number pairs.
pub async fn send_request(
    client: &reqwest::Client,
    url: StatusUrl,
    pois: impl IntoIterator<Item = &(DeploymentId, BlockNumber)>,
) -> Result<Vec<PublicProofOfIndexingResult>, Error> {
    let resp = client
        .post(url.into_inner())
        .send_graphql::<Response>(Request::new(pois))
        .await
        .map_err(|err| match err {
            RequestError::RequestSerializationError(..) => {
                unreachable!("request serialization should not fail")
            }
            RequestError::RequestSendError(..) | RequestError::ResponseRecvError(..) => {
                Error::Request(err.to_string())
            }
            RequestError::ResponseDeserializationError { .. } => {
                Error::InvalidResponse(err.to_string())
            }
        })?
        .map_err(|err| match err {
            ResponseError::Failure { .. } => Error::Request(err.to_string()),
            ResponseError::Empty => Error::EmptyResponse,
        })?;

    if resp.public_proofs_of_indexing.is_empty() {
        return Err(Error::EmptyResponse);
    }

    Ok(resp.public_proofs_of_indexing)
}

#[derive(Clone, Debug)]
pub struct Request {
    document: Document,
    var_requests: Vec<PublicProofOfIndexingRequest>,
}

impl Request {
    pub fn new<'a>(requests: impl IntoIterator<Item = &'a (DeploymentId, BlockNumber)>) -> Self {
        Self {
            document: PUBLIC_PROOF_OF_INDEXING_QUERY_DOCUMENT.into_document(),
            var_requests: requests.into_iter().copied().map(Into::into).collect(),
        }
    }
}

impl IntoDocumentWithVariables for Request {
    type Variables = serde_json::Value;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (
            self.document,
            serde_json::json!({ "requests": self.var_requests }),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    public_proofs_of_indexing: Vec<PublicProofOfIndexingResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicProofOfIndexingResult {
    pub deployment: DeploymentId,
    pub block: PartialBlockPtr,
    pub proof_of_indexing: Option<ProofOfIndexing>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct PartialBlockPtr {
    #[serde_as(as = "DisplayFromStr")]
    pub number: BlockNumber,
}

#[cfg(test)]
mod tests {
    use thegraph_core::{deployment_id, poi};

    use super::Response;

    #[test]
    fn deserialize_public_pois_response() {
        //* Given
        let response = r#"{
            "publicProofsOfIndexing": [
                {
                    "deployment": "QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH",
                    "proofOfIndexing": "0xba8a057796a81e013789789996551bb5b2920fb9947334db956992f7098bd287",
                    "block": {
                        "number": "123"
                    }
                },
                {
                    "deployment": "QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw",
                    "block": {
                        "number": "456"
                    }
                }
            ]
        }"#;

        //* When
        let response = serde_json::from_str::<Response>(response);

        //* Then
        let response = response.expect("deserialization failed");

        assert_eq!(response.public_proofs_of_indexing.len(), 2);
        assert_eq!(
            response.public_proofs_of_indexing[0].deployment,
            deployment_id!("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH")
        );
        assert_eq!(
            response.public_proofs_of_indexing[0].proof_of_indexing,
            Some(poi!(
                "ba8a057796a81e013789789996551bb5b2920fb9947334db956992f7098bd287"
            ))
        );
        assert_eq!(response.public_proofs_of_indexing[0].block.number, 123);
        assert_eq!(
            response.public_proofs_of_indexing[1].deployment,
            deployment_id!("QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw")
        );
        assert_eq!(
            response.public_proofs_of_indexing[1].proof_of_indexing,
            None
        );
        assert_eq!(response.public_proofs_of_indexing[1].block.number, 456);
    }
}
