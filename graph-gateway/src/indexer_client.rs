use std::sync::Arc;

use alloy_primitives::BlockNumber;
use indexer_selection::Selection;
use serde::Deserialize;
use thegraph::types::attestation::Attestation;

use crate::receipts::{ReceiptSigner, ReceiptStatus, ScalarReceipt};
use gateway_common::errors::{IndexerError, UnavailableReason::*};

pub struct IndexerResponse {
    pub status: u16,
    pub receipt: ScalarReceipt,
    pub payload: ResponsePayload,
}

#[derive(Clone, Debug)]
pub struct ResponsePayload {
    pub body: Arc<String>,
    pub attestation: Option<Attestation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockError {
    pub unresolved: Option<BlockNumber>,
    pub reported_status: Option<BlockNumber>,
}

#[derive(Debug, Deserialize)]
pub struct IndexerResponsePayload {
    #[serde(rename = "graphQLResponse")]
    pub graphql_response: Option<String>,
    pub attestation: Option<Attestation>,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct IndexerClient {
    pub client: reqwest::Client,
    pub receipt_signer: &'static ReceiptSigner,
}

impl IndexerClient {
    pub async fn query_indexer(
        &self,
        selection: &Selection,
        query: String,
    ) -> Result<IndexerResponse, IndexerError> {
        let url = selection
            .url
            .join(&format!("subgraphs/id/{:?}", selection.indexing.deployment))
            .map_err(|_| IndexerError::Unavailable(NoStatus))?;
        let receipt = self
            .receipt_signer
            .create_receipt(&selection.indexing, &selection.fee)
            .await
            .ok_or(IndexerError::Internal("failed to create receipt"))?;

        let result = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .header("Scalar-Receipt", &receipt.serialize())
            .body(query)
            .send()
            .await
            .and_then(|response| response.error_for_status());

        let receipt_status = match &result {
            Ok(_) => ReceiptStatus::Success,
            Err(err) if err.is_timeout() => ReceiptStatus::Unknown,
            Err(_) => ReceiptStatus::Failure,
        };
        self.receipt_signer
            .record_receipt(&selection.indexing, &receipt, receipt_status)
            .await;

        let response = match result {
            Ok(response) => response,
            Err(err) if err.is_timeout() => return Err(IndexerError::Timeout),
            Err(err) => match err.status() {
                Some(status) => return Err(IndexerError::BadResponse(status.as_u16().to_string())),
                _ if err.is_connect() => {
                    return Err(IndexerError::BadResponse("failed to connect".to_string()))
                }
                _ => return Err(IndexerError::BadResponse(err.to_string())),
            },
        };
        let response_status = response.status();
        let payload = response
            .json::<IndexerResponsePayload>()
            .await
            .map_err(|err| IndexerError::BadResponse(err.to_string()))?;
        let graphql_response = match payload.graphql_response {
            Some(graphql_response) => graphql_response,
            None => {
                let err = payload
                    .error
                    .unwrap_or_else(|| "missing GraphQL response".to_string());
                return Err(IndexerError::BadResponse(err));
            }
        };
        Ok(IndexerResponse {
            status: response_status.as_u16(),
            receipt,
            payload: ResponsePayload {
                body: Arc::new(graphql_response),
                attestation: payload.attestation,
            },
        })
    }
}

pub fn check_block_error(err: &str) -> Result<(), BlockError> {
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
    Err(BlockError {
        unresolved: extract_block_number("and data for block number "),
        reported_status: extract_block_number("has only indexed up to block number "),
    })
}

#[cfg(test)]
mod test {
    use crate::indexer_client::BlockError;

    #[test]
    fn check_block_error() {
        let tests = [
            ("", Ok(())),
            ("Failed to decode `block.number` value: `subgraph QmQqLJVgZLcRduoszARzRi12qGheUTWAHFf3ixMeGm2xML has only indexed up to block number 133239690 and data for block number 133239697 is therefore not yet available", Err(BlockError {
                unresolved: Some(133239697),
                reported_status: Some(133239690),
            })),
            ("Failed to decode `block.hash` value", Err(BlockError {
                unresolved: None,
                reported_status: None,
            })),
        ];
        for (input, expected) in tests {
            assert_eq!(super::check_block_error(input), expected);
        }
    }
}
