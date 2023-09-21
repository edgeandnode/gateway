use std::sync::Arc;

use alloy_primitives::B256;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use indexer_selection::{Selection, UnresolvedBlock};

#[derive(Debug)]
pub struct IndexerResponse {
    pub status: u16,
    pub payload: ResponsePayload,
}

#[derive(Clone, Debug)]
pub struct ResponsePayload {
    pub body: Arc<String>,
    pub attestation: Option<Attestation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexerError {
    NoAllocation,
    NoAttestation,
    UnattestableError(StatusCode),
    Timeout,
    UnexpectedPayload,
    UnresolvedBlock,
    Other(String),
}

impl From<UnresolvedBlock> for IndexerError {
    fn from(_: UnresolvedBlock) -> Self {
        Self::UnresolvedBlock
    }
}

#[derive(Debug, Deserialize)]
pub struct IndexerResponsePayload {
    #[serde(rename = "graphQLResponse")]
    pub graphql_response: Option<String>,
    pub attestation: Option<Attestation>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Attestation {
    #[serde(rename = "requestCID")]
    pub request_cid: B256,
    #[serde(rename = "responseCID")]
    pub response_cid: B256,
    #[serde(rename = "subgraphDeploymentID")]
    pub deployment: B256,
    pub v: u8,
    pub r: B256,
    pub s: B256,
}

#[derive(Clone)]
pub struct IndexerClient {
    pub client: reqwest::Client,
}

impl IndexerClient {
    #[tracing::instrument(skip_all)]
    pub async fn query_indexer(
        &self,
        selection: &Selection,
        query: String,
        receipt: &[u8],
    ) -> Result<IndexerResponse, IndexerError> {
        let receipt = hex::encode(receipt);
        let receipt = &receipt[0..(receipt.len() - 64)];
        let url = selection
            .url
            .join(&format!("subgraphs/id/{:?}", selection.indexing.deployment))
            .map_err(|err| IndexerError::Other(err.to_string()))?;
        let result = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .header("Scalar-Receipt", receipt)
            .body(query)
            .send()
            .await
            .and_then(|response| response.error_for_status());
        let response = match result {
            Ok(response) => response,
            // We need to observe timeouts differently in the ISA, so we discriminate them here.
            Err(err) if err.is_timeout() => return Err(IndexerError::Timeout),
            Err(err) => {
                tracing::trace!(response_status = ?err.status());
                return match err.status() {
                    Some(status) if status.is_server_error() => {
                        Err(IndexerError::UnattestableError(status))
                    }
                    _ => Err(IndexerError::Other(err.to_string())),
                };
            }
        };
        let response_status = response.status();
        tracing::trace!(%response_status);
        let payload = response
            .json::<IndexerResponsePayload>()
            .await
            .map_err(|err| IndexerError::Other(err.to_string()))?;
        let graphql_response = match payload.graphql_response {
            Some(graphql_response) => graphql_response,
            None => {
                let err = payload
                    .error
                    .unwrap_or_else(|| "GraphQL response not found".to_string());
                return Err(IndexerError::Other(err));
            }
        };
        tracing::debug!(response_len = graphql_response.len());
        Ok(IndexerResponse {
            status: response_status.as_u16(),
            payload: ResponsePayload {
                body: Arc::new(graphql_response),
                attestation: payload.attestation,
            },
        })
    }
}
