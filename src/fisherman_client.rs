use crate::{indexer_client::Attestation, prelude::*};
use async_trait::async_trait;
use reqwest;
use serde::Deserialize;
use serde_json::json;
use std::error::Error;
use url::Url;

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum ChallengeOutcome {
    AgreeWithTrustedIndexer,
    DisagreeWithTrustedIndexer,
    DisagreeWithUntrustedIndexer,
    FailedToProvideAttestation,
    Unknown,
}

#[async_trait]
pub trait FishermanInterface {
    async fn challenge(
        &self,
        indexer: &Address,
        allocation: &Address,
        indexer_query: &str,
        attestation: &Attestation,
    ) -> ChallengeOutcome;
}

#[derive(Clone)]
pub struct FishermanClient {
    client: reqwest::Client,
    url: Url,
}

#[async_trait]
impl FishermanInterface for FishermanClient {
    async fn challenge(
        &self,
        indexer: &Address,
        allocation: &Address,
        indexer_query: &str,
        attestation: &Attestation,
    ) -> ChallengeOutcome {
        match self
            .send_challenge(indexer, allocation, indexer_query, attestation)
            .await
        {
            Ok(outcome) => outcome,
            Err(fisherman_challenge_err) => {
                tracing::error!(%fisherman_challenge_err);
                ChallengeOutcome::Unknown
            }
        }
    }
}

impl FishermanClient {
    pub fn new(client: reqwest::Client, url: Url) -> Self {
        Self { client, url }
    }

    async fn send_challenge(
        &self,
        indexer: &Address,
        allocation: &Address,
        indexer_query: &str,
        attestation: &Attestation,
    ) -> Result<ChallengeOutcome, Box<dyn Error>> {
        let challenge = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "challenge",
            "params": {
                "readOperation": indexer_query,
                "allocationID": allocation.to_string(),
                "attestation": serde_json::to_value(attestation)?,
            },
        }))?;
        tracing::trace!(%indexer, %challenge);
        self.client
            .post(self.url.clone())
            .header("Content-Type", "application/json")
            .body(challenge)
            .send()
            .await?
            .json::<RPCResponse>()
            .await
            .map(|response| response.result)
            .map_err(Into::into)
    }
}

#[derive(Deserialize)]
struct RPCResponse {
    result: ChallengeOutcome,
}
