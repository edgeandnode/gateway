use alloy_primitives::Address;
use anyhow::{anyhow, Context};
use gateway_common::utils::timestamp::unix_timestamp;
use ordered_float::NotNan;
use prost::Message;
use serde_json::json;
use thegraph_core::types::DeploymentId;
use tokio::sync::mpsc;
use toolshed::concat_bytes;

use crate::{
    errors,
    gateway::http::gateway::{DeterministicRequest, IndexerResponse},
};

pub struct ClientRequest {
    pub id: String,
    pub response_time_ms: u16,
    pub result: Result<(), errors::Error>,
    pub api_key: String,
    pub user_address: Address,
    pub grt_per_usd: NotNan<f64>,
    pub indexer_requests: Vec<IndexerRequest>,
}

pub struct IndexerRequest {
    pub indexer: Address,
    pub url: String,
    pub deployment: DeploymentId,
    pub allocation: Address,
    pub subgraph_chain: String,
    pub result: Result<IndexerResponse, errors::IndexerError>,
    pub response_time_ms: u16,
    pub seconds_behind: u32,
    pub blocks_behind: u64, // TODO: rm
    pub legacy_scalar: bool,
    pub fee: u128,
    pub request: DeterministicRequest,
}

pub struct Reporter {
    pub graph_env: String,
    pub client_request_topic: String,
    pub indexer_request_topic: String,
    pub attestation_topic: String,
    pub write_buf: Vec<u8>,
    pub kafka_producer: rdkafka::producer::ThreadedProducer<
        rdkafka::producer::DefaultProducerContext,
        rdkafka::producer::NoCustomPartitioner,
    >,
}

impl Reporter {
    pub fn create(
        graph_env: String,
        client_request_topic: String,
        indexer_request_topic: String,
        attestation_topic: String,
        kafka_config: &rdkafka::ClientConfig,
    ) -> anyhow::Result<mpsc::UnboundedSender<ClientRequest>> {
        let kafka_producer = kafka_config.create().context("kafka producer error")?;
        let mut reporter = Self {
            graph_env,
            client_request_topic,
            indexer_request_topic,
            attestation_topic,
            write_buf: Default::default(),
            kafka_producer,
        };

        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let msg = rx.recv().await.expect("channel closed");
                if let Err(report_err) = reporter.report(msg) {
                    tracing::error!(%report_err);
                }
            }
        });
        Ok(tx)
    }

    fn report(&mut self, client_request: ClientRequest) -> anyhow::Result<()> {
        let timestamp = unix_timestamp();

        let total_fees_grt: f64 = client_request
            .indexer_requests
            .iter()
            .map(|i| i.fee as f64 * 1e-18)
            .sum();
        let total_fees_usd: f64 = total_fees_grt / *client_request.grt_per_usd;

        let client_request_payload = json!({
            "query_id": &client_request.id,
            "ray_id": &client_request.id,
            "graph_env": &self.graph_env,
            "timestamp": timestamp,
            "api_key": &client_request.api_key,
            "user": &client_request.user_address,
            "deployment": client_request.indexer_requests.first().map(|i| i.deployment.to_string()).unwrap_or_default(),
            "network": client_request.indexer_requests.first().map(|i| i.subgraph_chain.as_str()).unwrap_or(""),
            "response_time_ms": client_request.response_time_ms,
            "query_count": 1,
            "fee": total_fees_grt as f32,
            "fee_usd": total_fees_usd as f32,
            "status": client_request.result.as_ref().map(|()| "200 OK".into()).unwrap_or_else(|err| err.to_string()),
        });

        for indexer_request in client_request.indexer_requests {
            let indexer_errors = indexer_request
                .result
                .as_ref()
                .map(|r| {
                    r.errors
                        .iter()
                        .map(|err| err.as_str())
                        .collect::<Vec<&str>>()
                        .join("; ")
                })
                .unwrap_or_default();
            let indexer_request_payload = json!({
                "query_id": &client_request.id,
                "ray_id": &client_request.id,
                "graph_env": &self.graph_env,
                "timestamp": timestamp,
                "api_key": &client_request.api_key,
                "user_address": &client_request.user_address,
                "deployment": &indexer_request.deployment,
                "network": &indexer_request.subgraph_chain,
                "indexer": &indexer_request.indexer,
                "url": &indexer_request.url,
                "fee": (indexer_request.fee as f64 * 1e-18) as f32,
                "legacy_scalar": indexer_request.legacy_scalar,
                "utility": 1.0,
                "blocks_behind": indexer_request.blocks_behind,
                "response_time_ms": indexer_request.response_time_ms,
                "allocation": &indexer_request.allocation,
                "indexer_errors": indexer_errors,
                "status": indexer_request.result.as_ref().map(|_| "200 OK".into()).unwrap_or_else(|err| err.to_string()),
            });
            serde_json::to_writer(&mut self.write_buf, &indexer_request_payload).unwrap();
            let record: rdkafka::producer::BaseRecord<(), [u8], ()> =
                rdkafka::producer::BaseRecord::to(&self.indexer_request_topic)
                    .payload(&self.write_buf);
            self.kafka_producer
                .send(record)
                .map_err(|(err, _)| err)
                .context(anyhow!(
                    "failed to send to topic {}",
                    self.indexer_request_topic
                ))?;
            self.write_buf.clear();

            if let Some((original_response, attestation)) = indexer_request
                .result
                .ok()
                .and_then(|r| Some((r.original_response, r.attestation?)))
            {
                const MAX_PAYLOAD_BYTES: usize = 10_000;
                AttestationProtobuf {
                    request: Some(indexer_request.request)
                        .filter(|r| r.body.len() <= MAX_PAYLOAD_BYTES)
                        .map(|r| r.body),
                    response: Some(original_response).filter(|r| r.len() <= MAX_PAYLOAD_BYTES),
                    allocation: indexer_request.allocation.0 .0.into(),
                    subgraph_deployment: attestation.deployment.0.into(),
                    request_cid: attestation.request_cid.0.into(),
                    response_cid: attestation.response_cid.0.into(),
                    signature: concat_bytes!(
                        65,
                        [&[attestation.v], &attestation.r.0, &attestation.s.0]
                    )
                    .into(),
                }
                .encode(&mut self.write_buf)
                .unwrap();
                let record: rdkafka::producer::BaseRecord<(), [u8], ()> =
                    rdkafka::producer::BaseRecord::to(&self.attestation_topic)
                        .payload(&self.write_buf);
                self.kafka_producer
                    .send(record)
                    .map_err(|(err, _)| err)
                    .context(anyhow!(
                        "failed to send to topic {}",
                        self.attestation_topic
                    ))?;
                self.write_buf.clear();
            }
        }

        serde_json::to_writer(&mut self.write_buf, &client_request_payload).unwrap();
        let record: rdkafka::producer::BaseRecord<(), [u8], ()> =
            rdkafka::producer::BaseRecord::to(&self.client_request_topic).payload(&self.write_buf);
        self.kafka_producer
            .send(record)
            .map_err(|(err, _)| err)
            .context(anyhow!(
                "failed to send to topic {}",
                self.client_request_topic
            ))?;
        self.write_buf.clear();

        Ok(())
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct AttestationProtobuf {
    #[prost(string, optional, tag = "1")]
    request: Option<String>,
    #[prost(string, optional, tag = "2")]
    response: Option<String>,
    /// 20 bytes
    #[prost(bytes, tag = "3")]
    allocation: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "4")]
    subgraph_deployment: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "5")]
    request_cid: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "6")]
    response_cid: Vec<u8>,
    /// 65 bytes, ECDSA signature (v, r, s)
    #[prost(bytes, tag = "7")]
    signature: Vec<u8>,
}
