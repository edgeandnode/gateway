use async_trait::async_trait;
use hex;
use indexer_selection::{IndexerError, IndexerQuery};
use prelude::*;
use reqwest;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait IndexerInterface {
    async fn query_indexer(&self, query: &IndexerQuery) -> Result<IndexerResponse, IndexerError>;
}

#[derive(Clone, Debug)]
pub struct IndexerResponse {
    pub status: u16,
    pub payload: String,
    pub attestation: Option<Attestation>,
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
    pub request_cid: Bytes32,
    #[serde(rename = "responseCID")]
    pub response_cid: Bytes32,
    #[serde(rename = "subgraphDeploymentID")]
    pub deployment: Bytes32,
    pub v: u8,
    pub r: Bytes32,
    pub s: Bytes32,
}

#[derive(Clone)]
pub struct IndexerClient {
    pub client: reqwest::Client,
}

#[async_trait]
impl IndexerInterface for IndexerClient {
    #[tracing::instrument(skip(self, query))]
    async fn query_indexer(&self, query: &IndexerQuery) -> Result<IndexerResponse, IndexerError> {
        let receipt = hex::encode(&query.receipt.commitment);
        let receipt = &receipt[0..(receipt.len() - 64)];
        let url = query
            .score
            .url
            .join(&format!("/subgraphs/id/{:?}", query.indexing.deployment))
            .map_err(|err| IndexerError::Other(err.to_string()))?;
        let result = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .header("Scalar-Receipt", receipt)
            .body(query.query.clone())
            .send()
            .await;
        // We need to observe timeouts differently in the ISA, so we discriminate them here.
        let response = match result {
            Ok(response) => response,
            Err(err) if err.is_timeout() => return Err(IndexerError::Timeout),
            Err(err) => return Err(IndexerError::Other(err.to_string())),
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
        Ok(IndexerResponse {
            status: response_status.as_u16(),
            payload: graphql_response,
            attestation: payload.attestation,
        })
    }
}
