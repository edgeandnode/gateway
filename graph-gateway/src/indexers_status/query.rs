use indoc::indoc;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};

use prelude::{anyhow, bytes_wrapper, faster_hex, DeploymentId};

use crate::indexers_status::graphql::IntoGraphqlQuery;

pub const MAX_REQUESTS_PER_QUERY: usize = 1; // TODO: Increase to 10 after indexer v0.32 is released.

pub type BlockNumber = u64;

bytes_wrapper!(pub, ProofOfIndexing, 32, "HexStr");

#[derive(Clone, Debug)]
pub struct PublicProofOfIndexingQuery {
    pub requests: Vec<PublicProofOfIndexingRequest>,
}

#[derive(Clone, Debug)]
pub struct PublicProofOfIndexingRequest {
    pub deployment: DeploymentId,
    pub block_number: BlockNumber,
}

impl PublicProofOfIndexingRequest {
    fn to_query_params(&self) -> String {
        format!(
            r#"{{ deployment: "{}", blockNumber: "{}" }}"#,
            self.deployment, self.block_number
        )
    }
}

impl IntoGraphqlQuery for PublicProofOfIndexingQuery {
    fn to_query(&self) -> String {
        debug_assert!(!self.requests.is_empty(), "Must have at least one request");

        let requests = self
            .requests
            .iter()
            .map(|request| request.to_query_params())
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            indoc! {
                r#"{{
                    publicProofsOfIndexing(requests: [{}]) {{
                        deployment
                        proofOfIndexing
                        block {{ number }}
                    }}
                }}"#
            },
            requests
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
        use super::*;

        #[test]
        fn public_proof_of_indexing_request_to_query_params_format() {
            //// Given
            let request = PublicProofOfIndexingRequest {
                deployment: "QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw"
                    .parse()
                    .expect("Failed to parse deployment ID"),
                block_number: 123,
            };

            //// When
            let query = request.to_query_params();

            //// Then
            assert_eq!(
                query.as_str(),
                "{ deployment: \"QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw\", blockNumber: \"123\" }"
            );
        }

        #[test]
        fn create_status_public_pois_query() {
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
            let query = query.to_query();

            //// Then
            assert_eq!(
                query.as_str(),
                indoc! { r#"{
                    publicProofsOfIndexing(requests: [{ deployment: "QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw", blockNumber: "123" }, { deployment: "QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH", blockNumber: "456" }]) {
                        deployment
                        proofOfIndexing
                        block { number }
                    }
                }"# }
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
