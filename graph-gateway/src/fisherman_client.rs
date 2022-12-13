use crate::indexer_client::Attestation;
use indexer_selection::Indexing;
use prelude::*;
use serde::Deserialize;
use serde_json::json;

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum ChallengeOutcome {
    AgreeWithTrustedIndexer,
    DisagreeWithTrustedIndexer,
    DisagreeWithUntrustedIndexer,
    FailedToProvideAttestation,
    Unknown,
}

#[derive(Clone)]
pub struct FishermanClient {
    client: reqwest::Client,
    url: URL,
}

impl FishermanClient {
    pub async fn challenge(
        &self,
        indexing: &Indexing,
        allocation: &Address,
        indexer_query: &str,
        indexer_response: &str,
        attestation: &Attestation,
    ) -> ChallengeOutcome {
        match self
            .send_challenge(
                indexing,
                allocation,
                indexer_query,
                indexer_response,
                attestation,
            )
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
    pub fn new(client: reqwest::Client, url: URL) -> Self {
        Self { client, url }
    }

    async fn send_challenge(
        &self,
        indexing: &Indexing,
        allocation: &Address,
        indexer_query: &str,
        indexer_response: &str,
        attestation: &Attestation,
    ) -> anyhow::Result<ChallengeOutcome> {
        let challenge = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "challenge",
            "params": {
                "attestation": serde_json::to_value(attestation)?,
                "subgraphDeploymentID": format!("0x{}", hex::encode(indexing.deployment.0)),
                "allocationID": allocation.to_string(),
                "query": indexer_query,
                "response": indexer_response,
            },
        }))?;
        tracing::trace!(?indexing, %challenge);
        self.client
            .post(self.url.0.clone())
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
