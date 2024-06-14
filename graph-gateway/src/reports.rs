use alloy_primitives::Address;
use anyhow::{anyhow, Context};
use gateway_common::timestamp::unix_timestamp;
use gateway_framework::{errors, scalar::Receipt};
use ordered_float::NotNan;
use prost::Message;
use serde_json::json;
use thegraph_core::types::DeploymentId;
use tokio::sync::mpsc;
use toolshed::concat_bytes;

use crate::indexer_client::IndexerResponse;

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
    pub deployment: DeploymentId,
    pub largest_allocation: Address,
    pub url: String,
    pub receipt: Receipt,
    pub subgraph_chain: String,
    pub result: Result<IndexerResponse, errors::IndexerError>,
    pub response_time_ms: u16,
    pub seconds_behind: u32,
    pub blocks_behind: u64, // TODO: rm
    pub request: String,
}

pub struct Reporter {
    pub graph_env: String,
    pub budget: String,
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
        budget: String,
        client_request_topic: String,
        indexer_request_topic: String,
        attestation_topic: String,
        kafka_config: &rdkafka::ClientConfig,
    ) -> anyhow::Result<mpsc::UnboundedSender<ClientRequest>> {
        let kafka_producer = kafka_config.create().context("kafka producer error")?;
        let mut reporter = Self {
            graph_env,
            budget,
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
            .map(|i| i.receipt.grt_value() as f64 * 1e-18)
            .sum();
        let total_fees_usd: f64 = total_fees_grt / *client_request.grt_per_usd;

        let (legacy_status_message, legacy_status_code): (String, u32) =
            match &client_request.result {
                Ok(_) => ("200 OK".to_string(), 0),
                Err(err) => match err {
                    errors::Error::BlockNotFound(_) => ("Unresolved block".to_string(), 604610595),
                    errors::Error::Internal(_) => ("Internal error".to_string(), 816601499),
                    errors::Error::Auth(_) => ("Invalid API key".to_string(), 888904173),
                    errors::Error::BadQuery(_) => ("Invalid query".to_string(), 595700117),
                    errors::Error::NoIndexers => (
                        "No indexers found for subgraph deployment".to_string(),
                        1621366907,
                    ),
                    errors::Error::BadIndexers(_) => (
                        "No suitable indexer found for subgraph deployment".to_string(),
                        510359393,
                    ),
                    errors::Error::SubgraphNotFound(_) => (err.to_string(), 2599148187),
                },
            };

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
            "budget": self.budget.to_string(),
            "query_count": 1,
            "fee": total_fees_grt as f32,
            "fee_usd": total_fees_usd as f32,
            "status": legacy_status_message,
            "status_code": legacy_status_code,
        });

        let silly_old_timestamp =
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        // TODO: remove this println as soon as data science stops relying on it.
        println!(
            "{}",
            serde_json::to_string(&json!({
                "level": "INFO",
                "timestamp": silly_old_timestamp,
                "fields": {
                    "message": "Client query result",
                    "query_id": &client_request.id,
                    "ray_id": &client_request.id,
                    "deployment": client_request.indexer_requests.first().map(|i| i.deployment.to_string()).unwrap_or_default(),
                    "network": client_request.indexer_requests.first().map(|i| i.subgraph_chain.as_str()).unwrap_or(""),
                    "user": &client_request.user_address,
                    "api_key": &client_request.api_key,
                    "query_count": 1,
                    "budget": self.budget,
                    "fee": total_fees_grt as f32,
                    "fee_usd": total_fees_usd as f32,
                    "response_time_ms": client_request.response_time_ms,
                    "status": legacy_status_message,
                    "status_code": legacy_status_code,
                },
            }))
            .unwrap()
        );

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
            let legacy_status_code: u32 = {
                let (prefix, data) = match &indexer_request.result {
                    Ok(_) => (0x0, 200_u32.to_be()),
                    Err(errors::IndexerError::Internal(_)) => (0x1, 0x0),
                    Err(errors::IndexerError::Unavailable(_)) => (0x2, 0x0),
                    Err(errors::IndexerError::Timeout) => (0x3, 0x0),
                    Err(errors::IndexerError::BadResponse(_)) => (0x4, 0x0),
                };
                (prefix << 28) | (data & (u32::MAX >> 4))
            };

            // TODO: remove this println as soon as data science stops relying on it.
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "level": "INFO",
                    "timestamp": silly_old_timestamp,
                    "fields": {
                        "message": "Indexer attempt",
                        "query_id": &client_request.id,
                        "ray_id": &client_request.id,
                        "deployment": indexer_request.deployment.to_string(),
                        "indexer": &indexer_request.indexer,
                        "url": &indexer_request.url,
                        "blocks_behind": indexer_request.blocks_behind,
                        "attempt_index": 0,
                        "api_key": &client_request.api_key,
                        "fee": total_fees_grt as f32,
                        "response_time_ms": indexer_request.response_time_ms,
                        "allocation": &indexer_request.receipt.allocation(),
                        "indexer_errors": indexer_errors,
                        "status": indexer_request.result.as_ref().map(|_| "200 OK".into()).unwrap_or_else(|err| err.to_string()),
                        "status_code": legacy_status_code,
                    },
                }))
                .unwrap()
            );

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
                "fee": (indexer_request.receipt.grt_value() as f64 * 1e-18) as f32,
                "legacy_scalar": matches!(&indexer_request.receipt, Receipt::Legacy(_, _)),
                "utility": 1.0,
                "blocks_behind": indexer_request.blocks_behind,
                "response_time_ms": indexer_request.response_time_ms,
                "allocation": &indexer_request.receipt.allocation(),
                "indexer_errors": indexer_errors,
                "status": indexer_request.result.as_ref().map(|_| "200 OK".into()).unwrap_or_else(|err| err.to_string()),
                "status_code": legacy_status_code,
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
                    request: Some(indexer_request.request).filter(|r| r.len() <= MAX_PAYLOAD_BYTES),
                    response: Some(original_response).filter(|r| r.len() <= MAX_PAYLOAD_BYTES),
                    allocation: indexer_request.receipt.allocation().0 .0.into(),
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
