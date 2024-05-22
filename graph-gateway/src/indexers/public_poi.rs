use std::collections::HashMap;

use alloy_primitives::{BlockNumber, B256};
use indoc::indoc;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thegraph_core::types::DeploymentId;
use thegraph_graphql_http::{
    graphql::{Document, IntoDocument, IntoDocumentWithVariables},
    http_client::ReqwestExt,
};
use url::Url;

pub type ProofOfIndexing = B256;

#[derive(Debug, Clone, Eq, PartialEq, Hash, serde::Deserialize)]
pub struct ProofOfIndexingInfo {
    /// Proof of indexing (POI).
    pub proof_of_indexing: ProofOfIndexing,
    /// POI deployment ID (the IPFS Hash in the Graph Network Subgraph).
    pub deployment_id: DeploymentId,
    /// POI block number.
    pub block_number: BlockNumber,
}

impl ProofOfIndexingInfo {
    /// Get the POI bytes.
    pub fn poi(&self) -> ProofOfIndexing {
        self.proof_of_indexing
    }

    /// Get the POI metadata.
    pub fn meta(&self) -> (DeploymentId, BlockNumber) {
        (self.deployment_id, self.block_number)
    }
}

pub async fn query(
    client: reqwest::Client,
    status_url: Url,
    query: PublicProofOfIndexingQuery,
) -> anyhow::Result<PublicProofOfIndexingResponse> {
    let res = client.post(status_url).send_graphql(query).await;
    match res {
        Ok(res) => Ok(res?),
        Err(e) => Err(anyhow::anyhow!(
            "Error sending public proof of indexing query: {}",
            e
        )),
    }
}

pub async fn merge_queries(
    client: reqwest::Client,
    status_url: Url,
    requests: &[(DeploymentId, BlockNumber)],
    batch_size: usize,
) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
    // Build the query batches and create the futures
    let queries = requests
        .iter()
        .map(|(deployment, block_number)| PublicProofOfIndexingRequest {
            deployment: *deployment,
            block_number: *block_number,
        })
        .chunks(batch_size)
        .into_iter()
        .map(|requests| PublicProofOfIndexingQuery {
            requests: requests.collect(),
        })
        .map(|query| self::query(client.clone(), status_url.clone(), query))
        .collect::<Vec<_>>();

    // Send all queries concurrently
    let responses = futures::future::join_all(queries).await;

    let response_map: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> = responses
        .into_iter()
        .filter_map(|response| {
            response
                .map_err(|e| tracing::trace!("Error querying public proof of indexing: {}", e))
                .ok()
        })
        .flat_map(|response| response.public_proofs_of_indexing)
        .filter_map(|response| {
            // If the response is missing the POI field, skip it.
            let poi = response.proof_of_indexing?;
            Some(((response.deployment, response.block.number), poi))
        })
        .collect::<HashMap<_, _>>();

    response_map
}

pub const MAX_REQUESTS_PER_QUERY: usize = 10;

pub(super) const PUBLIC_PROOF_OF_INDEXING_QUERY_DOCUMENT: &str = indoc! {
    r#"query ($requests: [PublicProofOfIndexingRequest!]!) {
        publicProofsOfIndexing(requests: $requests) {
            deployment
            proofOfIndexing
            block {
                number
            }
        }
    }"#
};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicProofOfIndexingQuery {
    pub requests: Vec<PublicProofOfIndexingRequest>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicProofOfIndexingRequest {
    pub deployment: DeploymentId,
    pub block_number: BlockNumber,
}

impl IntoDocumentWithVariables for PublicProofOfIndexingQuery {
    type Variables = Self;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        debug_assert!(!self.requests.is_empty(), "Must have at least one request");

        (
            PUBLIC_PROOF_OF_INDEXING_QUERY_DOCUMENT.into_document(),
            self,
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicProofOfIndexingResponse {
    pub public_proofs_of_indexing: Vec<PublicProofOfIndexingResult>,
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
    use super::*;

    mod query {
        use serde_json::json;
        use thegraph_graphql_http::http::request::IntoRequestParameters;

        use super::*;

        #[test]
        fn create_status_public_pois_request_params() {
            //// Given
            let query = PublicProofOfIndexingQuery {
                requests: vec![
                    PublicProofOfIndexingRequest {
                        deployment: "QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw"
                            .parse()
                            .expect("Failed to parse deployment ID"),
                        block_number: 123,
                    },
                    PublicProofOfIndexingRequest {
                        deployment: "QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"
                            .parse()
                            .expect("Failed to parse deployment ID"),
                        block_number: 456,
                    },
                ],
            };

            //// When
            let query = query.into_request_parameters();

            //// Then
            // Assert the query document.
            assert_eq!(
                query.query.as_str(),
                PUBLIC_PROOF_OF_INDEXING_QUERY_DOCUMENT
            );

            // Assert the query variables.
            assert!(matches!(
                query.variables.get("requests"),
                Some(serde_json::Value::Array(_))
            ));

            let var_requests = query
                .variables
                .get("requests")
                .expect("Missing requests variables")
                .as_array()
                .expect("Invalid requests variables");
            assert_eq!(
                var_requests[0],
                json!({
                    "deployment": "QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw",
                    "blockNumber": 123
                })
            );
            assert_eq!(
                var_requests[1],
                json!({
                    "deployment": "QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH",
                    "blockNumber": 456
                })
            );
        }
    }

    mod response {
        use super::*;

        #[test]
        fn deserialize_public_pois_response() {
            //// Given
            let response = indoc! {
                r#"{
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
                }"#
            };

            //// When
            let response: PublicProofOfIndexingResponse =
                serde_json::from_str(response).expect("Failed to deserialize response");

            //// Then
            assert_eq!(response.public_proofs_of_indexing.len(), 2);
            assert_eq!(
                response.public_proofs_of_indexing[0].deployment,
                "QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"
                    .parse()
                    .unwrap()
            );
            assert_eq!(
                response.public_proofs_of_indexing[0].proof_of_indexing,
                Some(
                    "0xba8a057796a81e013789789996551bb5b2920fb9947334db956992f7098bd287"
                        .parse()
                        .unwrap()
                )
            );
            assert_eq!(response.public_proofs_of_indexing[0].block.number, 123);
            assert_eq!(
                response.public_proofs_of_indexing[1].deployment,
                "QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw"
                    .parse()
                    .unwrap()
            );
            assert_eq!(
                response.public_proofs_of_indexing[1].proof_of_indexing,
                None
            );
            assert_eq!(response.public_proofs_of_indexing[1].block.number, 456);
        }
    }
}
