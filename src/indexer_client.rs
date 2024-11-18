use alloy_sol_types::Eip712Domain;
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use thegraph_core::attestation::{self, Attestation};
use thegraph_graphql_http::http::response::Error as GQLError;
use url::Url;

use crate::{
    errors::{
        IndexerError::{self, *},
        MissingBlockError, UnavailableReason,
    },
    receipts::Receipt,
    unattestable_errors::miscategorized_unattestable,
};

#[derive(Clone, Debug)]
pub struct IndexerResponse {
    pub response: String,
    pub attestation: Option<Attestation>,
    pub errors: Vec<String>,
}

#[derive(Clone)]
pub struct IndexerClient {
    pub client: reqwest::Client,
}

pub enum IndexerAuth<'a> {
    Paid(&'a Receipt, &'a Eip712Domain),
    Free(&'a str),
}

impl IndexerClient {
    pub async fn query_indexer<'a>(
        &self,
        deployment_url: Url,
        auth: IndexerAuth<'a>,
        query: &str,
    ) -> Result<IndexerResponse, IndexerError> {
        let (auth_key, auth_value) = match auth {
            IndexerAuth::Paid(receipt, _) => (receipt.header_name(), receipt.serialize()),
            IndexerAuth::Free(token) => (AUTHORIZATION.as_str(), format!("Bearer {token}")),
        };

        let result = self
            .client
            .post(deployment_url)
            .header("Content-Type", "application/json")
            .header(auth_key, auth_value)
            .body(query.to_string())
            .send()
            .await;
        let response = match result.and_then(|r| r.error_for_status()) {
            Ok(response) => response,
            Err(err) if err.is_timeout() => return Err(Timeout),
            Err(err) => {
                return match err.status() {
                    Some(status) => Err(BadResponse(status.as_u16().to_string())),
                    _ if err.is_connect() => Err(BadResponse("failed to connect".to_string())),
                    _ => Err(BadResponse(err.to_string())),
                }
            }
        };

        #[derive(Debug, Deserialize)]
        pub struct IndexerResponsePayload {
            #[serde(rename = "graphQLResponse")]
            pub graphql_response: Option<String>,
            pub attestation: Option<Attestation>,
            pub error: Option<String>,
        }
        let payload = response
            .json::<IndexerResponsePayload>()
            .await
            .map_err(|err| BadResponse(err.to_string()))?;
        if let Some(err) = payload.error {
            return Err(BadResponse(err));
        }

        let response = payload
            .graphql_response
            .ok_or_else(|| BadResponse("missing response".into()))?;
        let errors = parse_indexer_errors(&response)?;

        errors
            .iter()
            .try_for_each(|err| check_block_error(err))
            .map_err(|err| Unavailable(UnavailableReason::MissingBlock(err)))?;
        if let Some(error) = errors
            .iter()
            .find(|error| miscategorized_unattestable(error))
        {
            return Err(BadResponse(format!("unattestable response: {error}")));
        }

        if let IndexerAuth::Paid(receipt, attestation_domain) = auth {
            match &payload.attestation {
                Some(attestation) => {
                    let allocation = receipt.allocation();
                    if let Err(err) = attestation::verify(
                        attestation_domain,
                        attestation,
                        &allocation,
                        query,
                        &response,
                    ) {
                        return Err(BadResponse(format!("bad attestation: {err}")));
                    }
                }
                None => {
                    let message = if !errors.is_empty() {
                        format!(
                            "no attestation: {}",
                            errors
                                .iter()
                                .map(|err| err.as_str())
                                .collect::<Vec<&str>>()
                                .join("; ")
                        )
                    } else {
                        "no attestation".to_string()
                    };
                    return Err(BadResponse(message));
                }
            };
        }

        Ok(IndexerResponse {
            response,
            attestation: payload.attestation,
            errors,
        })
    }
}

fn parse_indexer_errors(response: &str) -> Result<Vec<String>, IndexerError> {
    #[derive(Deserialize, Serialize)]
    struct ResponseErrors {
        #[serde(default)]
        #[serde(skip_serializing_if = "Vec::is_empty")]
        errors: Vec<GQLError>,
        // indexer-service sometimes returns errors in this form, which isn't ideal
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    }
    let mut payload: ResponseErrors =
        serde_json::from_str(response).map_err(|err| BadResponse(err.to_string()))?;

    if let Some(err) = payload.error.take() {
        payload.errors.push(GQLError {
            message: err,
            locations: Default::default(),
            path: Default::default(),
        });
    }

    // Avoid processing oversized errors.
    for err in &mut payload.errors {
        err.message.truncate(256);
        err.message.shrink_to_fit();
    }

    Ok(payload.errors.into_iter().map(|e| e.message).collect())
}

fn check_block_error(err: &str) -> Result<(), MissingBlockError> {
    // TODO: indexers should *always* report their block status in a header on every query. This
    // will significantly reduce how brittle this feedback is, and also give a stronger basis for
    // prediction in the happy path.
    if !err.contains("Failed to decode `block") {
        return Ok(());
    }
    let extract_block_number = |prefix: &str| -> Option<u64> {
        let start = err.find(prefix)? + prefix.len();
        let str = err.split_at(start).1.split_once(' ')?.0;
        str.parse::<u64>().ok()
    };
    Err(MissingBlockError {
        missing: extract_block_number("and data for block number "),
        latest: extract_block_number("has only indexed up to block number "),
    })
}

#[cfg(test)]
mod tests {
    use crate::errors::MissingBlockError;

    #[test]
    fn check_block_error() {
        let tests = [
            ("", Ok(())),
            ("Failed to decode `block.number` value: `subgraph QmQqLJVgZLcRduoszARzRi12qGheUTWAHFf3ixMeGm2xML has only indexed up to block number 133239690 and data for block number 133239697 is therefore not yet available", Err(MissingBlockError {
                missing: Some(133239697),
                latest: Some(133239690),
            })),
            ("Failed to decode `block.hash` value", Err(MissingBlockError {
                missing: None,
                latest: None,
            })),
        ];
        for (input, expected) in tests {
            assert_eq!(super::check_block_error(input), expected);
        }
    }
}
