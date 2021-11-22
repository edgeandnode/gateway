use crate::{indexer_selection::IndexerQuery, prelude::*, query_engine::*};
use async_trait::async_trait;
use hex;
use reqwest;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[async_trait]
pub trait IndexerInterface {
    async fn query_indexer(&self, query: &IndexerQuery) -> Result<IndexerResponse, Box<dyn Error>>;
}

#[derive(Debug, Deserialize)]
pub struct IndexerResponsePayload {
    #[serde(rename(deserialize = "graphQLResponse"))]
    pub graphql_response: String,
    pub attestation: Option<Attestation>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Attestation {
    #[serde(rename(deserialize = "requestCID"))]
    pub request_cid: Bytes32,
    #[serde(rename(deserialize = "responseCID"))]
    pub response_cid: Bytes32,
    #[serde(rename(deserialize = "subgraphDeploymentID"))]
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
    async fn query_indexer(&self, query: &IndexerQuery) -> Result<IndexerResponse, Box<dyn Error>> {
        let receipt = hex::encode(&query.receipt.commitment);
        let receipt = &receipt[0..(receipt.len() - 64)];
        let response = self
            .client
            .post(format!(
                "{}/subgraphs/id/{:?}",
                query.url, query.indexing.deployment
            ))
            .header("Content-Type", "application/json")
            .header("Scalar-Receipt", receipt)
            .body(query.query.clone())
            .send()
            .await?;
        let response_status = response.status();
        tracing::info!(%response_status);
        let payload = response.json::<IndexerResponsePayload>().await?;
        Ok(IndexerResponse {
            status: response_status.as_u16(),
            payload: payload.graphql_response,
            attestation: payload.attestation,
        })
    }
}
