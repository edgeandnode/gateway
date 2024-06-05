use alloy_primitives::{BlockHash, BlockNumber};
use alloy_sol_types::Eip712Domain;
use gateway_framework::{
    blocks::Block,
    errors::{
        IndexerError::{self, *},
        MissingBlockError,
        UnavailableReason::{self, *},
    },
    scalar::ScalarReceipt,
};
use serde::{Deserialize, Serialize};
use thegraph_core::types::{
    attestation::{self, Attestation},
    DeploymentId,
};
use thegraph_graphql_http::http::response::{Error as GQLError, ResponseBody as GQLResponseBody};
use url::Url;

use crate::unattestable_errors::miscategorized_unattestable;

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

impl IndexerClient {
    pub async fn query_indexer(
        &self,
        deployment: &DeploymentId,
        url: &Url,
        receipt: &ScalarReceipt,
        attestation_domain: &Eip712Domain,
        query: &str,
    ) -> Result<IndexerResponse, IndexerError> {
        let url = url
            .join(&format!("subgraphs/id/{:?}", deployment))
            .map_err(|_| Unavailable(NoStatus))?;

        let result = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .header(receipt.header_name(), receipt.serialize())
            .body(query.to_string())
            .send()
            .await
            .and_then(|response| response.error_for_status());

        let response = match result {
            Ok(response) => response,
            Err(err) if err.is_timeout() => return Err(Timeout),
            Err(err) => match err.status() {
                Some(status) => return Err(BadResponse(status.as_u16().to_string())),
                _ if err.is_connect() => return Err(BadResponse("failed to connect".to_string())),
                _ => return Err(BadResponse(err.to_string())),
            },
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

        match &payload.attestation {
            Some(attestation) => {
                let allocation = receipt.allocation();
                if let Err(err) = attestation::verify(
                    attestation_domain,
                    attestation,
                    &allocation,
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

        Ok(IndexerResponse {
            original_response,
            attestation: payload.attestation,
            client_response,
            errors,
            probe_block,
        })
    }
}

fn rewrite_response(
    response: &str,
) -> Result<(String, Vec<GQLError>, Option<Block>), IndexerError> {
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
    let mut payload: GQLResponseBody<ProbedData> =
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
mod test {
    use gateway_framework::errors::MissingBlockError;

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
