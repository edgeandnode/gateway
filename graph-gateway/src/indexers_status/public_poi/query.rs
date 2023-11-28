use alloy_primitives::{BlockNumber, B256};
use graphql_http::graphql::{Document, IntoDocument, IntoDocumentWithVariables};
use indoc::indoc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thegraph::types::DeploymentId;

pub const MAX_REQUESTS_PER_QUERY: usize = 10;

pub type ProofOfIndexing = B256;

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
        use graphql_http::http::request::IntoRequestParameters;
        use serde_json::json;

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
