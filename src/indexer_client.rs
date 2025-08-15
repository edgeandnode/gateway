use http::{StatusCode, header::CONTENT_TYPE};
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use thegraph_core::{
    alloy::{
        dyn_abi::Eip712Domain,
        primitives::{BlockHash, BlockNumber},
    },
    attestation::{self, Attestation},
};
use url::Url;

use crate::{
    blocks::Block,
    errors::{
        IndexerError::{self, *},
        MissingBlockError, UnavailableReason,
    },
    receipts::Receipt,
    unattestable_errors::miscategorized_unattestable,
};

#[derive(Clone, Debug)]
pub struct IndexerResponse {
    pub original_response: String,
    pub attestation: Option<Attestation>,
    pub client_response: String,
    pub errors: Vec<String>,
    pub probe_block: Option<Block>,
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
    pub async fn query_indexer(
        &self,
        deployment_url: Url,
        auth: IndexerAuth<'_>,
        query: &str,
    ) -> Result<IndexerResponse, IndexerError> {
        let (auth_key, auth_value) = match auth {
            IndexerAuth::Paid(receipt, _) => ("tap-receipt", receipt.serialize()),
            IndexerAuth::Free(token) => (AUTHORIZATION.as_str(), format!("Bearer {token}")),
        };

        // Debug logging to track actual HTTP request details
        tracing::debug!(
            url = %deployment_url,
            auth_header = auth_key,
            auth_value_length = auth_value.len(),
            query_length = query.len(),
            "sending HTTP POST request to indexer"
        );

        // CRITICAL DEBUG: Log if TAP receipt header is being set
        if auth_key == "tap-receipt" {
            tracing::warn!(
                url = %deployment_url,
                auth_value_preview = &auth_value[..auth_value.len().min(50)],
                "TAP RECEIPT HEADER BEING SENT TO INDEXER"
            );
        }

        let result = self
            .client
            .post(deployment_url.clone())
            .header(CONTENT_TYPE.as_str(), "application/json")
            .header(auth_key, auth_value)
            .body(query.to_string())
            .send()
            .await;
        let response = match result {
            Ok(response) => response,
            Err(e) if e.is_timeout() => return Err(Timeout),
            Err(e) if e.is_connect() => return Err(BadResponse("failed to connect".to_string())),
            Err(e) => return Err(BadResponse(e.to_string())),
        };
        let status = response.status();
        if status != StatusCode::OK {
            if let Ok(body) = response.text().await {
                tracing::info!(status = status.as_u16(), indexer_err_response = body);
            }
            return Err(BadResponse(status.as_u16().to_string()));
        }

        if let Some(indexed_block) = response
            .headers()
            .get("graph-indexed")
            .and_then(|v| v.to_str().ok())
        {
            tracing::debug!(indexed_block);
        }

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

        let original_response = payload
            .graphql_response
            .ok_or_else(|| BadResponse("missing response".into()))?;
        let (client_response, errors, probe_block) = rewrite_response(&original_response)?;
        let errors: Vec<String> = errors.into_iter().map(|err| err.message).collect();

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
                    let collection = receipt.collection();
                    if let Err(err) = attestation::verify(
                        attestation_domain,
                        attestation,
                        &collection.as_address(),
                        query,
                        &original_response,
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
            original_response,
            attestation: payload.attestation,
            client_response,
            errors,
            probe_block,
        })
    }
}

#[derive(Deserialize, Serialize)]
struct Error {
    message: String,
}

fn rewrite_response(response: &str) -> Result<(String, Vec<Error>, Option<Block>), IndexerError> {
    #[derive(Deserialize, Serialize)]
    struct Response {
        data: Option<ProbedData>,
        #[serde(default)]
        #[serde(skip_serializing_if = "Vec::is_empty")]
        errors: Vec<Error>,
    }
    #[derive(Deserialize, Serialize)]
    struct ProbedData {
        #[serde(rename = "_gateway_probe_", skip_serializing)]
        probe: Option<Meta>,
        #[serde(flatten)]
        data: serde_json::Value,
    }
    #[derive(Deserialize)]
    struct Meta {
        block: MaybeBlock,
    }
    #[derive(Deserialize)]
    struct MaybeBlock {
        number: BlockNumber,
        hash: BlockHash,
        timestamp: Option<u64>,
    }
    let mut payload: Response =
        serde_json::from_str(response).map_err(|err| BadResponse(err.to_string()))?;

    // Avoid processing oversized errors.
    for err in &mut payload.errors {
        err.message.truncate(256);
        err.message.shrink_to_fit();
    }

    let block = payload
        .data
        .as_mut()
        .and_then(|data| data.probe.take())
        .and_then(|meta| {
            Some(Block {
                number: meta.block.number,
                hash: meta.block.hash,
                timestamp: meta.block.timestamp?,
            })
        });
    let client_response = serde_json::to_string(&payload).unwrap();
    Ok((client_response, payload.errors, block))
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
            (
                "Failed to decode `block.number` value: `subgraph QmQqLJVgZLcRduoszARzRi12qGheUTWAHFf3ixMeGm2xML has only indexed up to block number 133239690 and data for block number 133239697 is therefore not yet available",
                Err(MissingBlockError {
                    missing: Some(133239697),
                    latest: Some(133239690),
                }),
            ),
            (
                "Failed to decode `block.hash` value",
                Err(MissingBlockError {
                    missing: None,
                    latest: None,
                }),
            ),
        ];
        for (input, expected) in tests {
            assert_eq!(super::check_block_error(input), expected);
        }
    }
}
