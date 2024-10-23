use anyhow::{anyhow, Context};
use ordered_float::NotNan;
use prost::Message;
use serde_json::json;
use thegraph_core::{Address, AllocationId, DeploymentId, IndexerId};
use tokio::sync::mpsc;
use toolshed::concat_bytes;

use crate::{errors, indexer_client::IndexerResponse, receipts::Receipt, time::unix_timestamp};

pub struct ClientRequest {
    pub id: String,
    pub response_time_ms: u16,
    pub result: Result<(), errors::Error>,
    pub api_key: String,
    pub user_address: String,
    pub grt_per_usd: NotNan<f64>,
    pub indexer_requests: Vec<IndexerRequest>,
    pub request_bytes: u32,
    pub response_bytes: Option<u32>,
}

pub struct IndexerRequest {
    pub indexer: IndexerId,
    pub deployment: DeploymentId,
    pub largest_allocation: AllocationId,
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
    pub tap_signer: Address,
    pub graph_env: String,
    pub budget: String,
    pub topics: Topics,
    pub write_buf: Vec<u8>,
    pub kafka_producer: rdkafka::producer::ThreadedProducer<
        rdkafka::producer::DefaultProducerContext,
        rdkafka::producer::NoCustomPartitioner,
    >,
}

pub struct Topics {
    pub client_request: &'static str,
    pub indexer_request: &'static str,
    pub attestation: &'static str,
    pub indexer_fees: &'static str,
}

impl Reporter {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        tap_signer: Address,
        graph_env: String,
        budget: NotNan<f64>,
        topics: Topics,
        kafka_config: impl Into<rdkafka::ClientConfig>,
    ) -> anyhow::Result<mpsc::UnboundedSender<ClientRequest>> {
        let kafka_producer = kafka_config
            .into()
            .create()
            .context("kafka producer error")?;
        let mut reporter = Self {
            tap_signer,
            graph_env,
            budget: budget.to_string(),
            topics,
            write_buf: Default::default(),
            kafka_producer,
        };

        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
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

        let indexed_chain = client_request
            .indexer_requests
            .first()
            .map(|i| i.subgraph_chain.as_str())
            .unwrap_or("");
        let gateway_id = format!("{:?}", self.tap_signer);
        let client_request_payload = json!({
            "gateway_id": &gateway_id,
            "query_id": &client_request.id,
            "ray_id": &client_request.id,
            "network_chain": &self.graph_env,
            "graph_env": &self.graph_env,
            "timestamp": timestamp,
            "api_key": &client_request.api_key,
            "user": &client_request.user_address,
            "deployment": client_request.indexer_requests.first().map(|i| i.deployment.to_string()).unwrap_or_default(),
            "indexed_chain": indexed_chain,
            "network": indexed_chain,
            "response_time_ms": client_request.response_time_ms,
            "request_bytes": client_request.request_bytes,
            "response_bytes": client_request.response_bytes,
            "budget": self.budget.to_string(),
            "query_count": 1,
            "fee": total_fees_grt as f32,
            "fee_usd": total_fees_usd as f32,
            "status": legacy_status_message,
            "status_code": legacy_status_code,
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

            let indexer_request_payload = json!({
                "gateway_id": &gateway_id,
                "query_id": &client_request.id,
                "ray_id": &client_request.id,
                "network_chain": &self.graph_env,
                "graph_env": &self.graph_env,
                "timestamp": timestamp,
                "api_key": &client_request.api_key,
                "user_address": &client_request.user_address,
                "deployment": &indexer_request.deployment,
                "network": &indexer_request.subgraph_chain,
                "indexed_chain": &indexer_request.subgraph_chain,
                "indexer": &indexer_request.indexer,
                "url": &indexer_request.url,
                "fee": (indexer_request.receipt.grt_value() as f64 * 1e-18) as f32,
                "legacy_scalar": matches!(&indexer_request.receipt, Receipt::Legacy(_, _)),
                "utility": 1.0,
                "seconds_behind": indexer_request.seconds_behind,
                "blocks_behind": indexer_request.blocks_behind,
                "response_time_ms": indexer_request.response_time_ms,
                "allocation": &indexer_request.receipt.allocation(),
                "indexer_errors": indexer_errors,
                "status": indexer_request.result.as_ref().map(|_| "200 OK".into()).unwrap_or_else(|err| err.to_string()),
                "status_code": legacy_status_code,
            });
            serde_json::to_writer(&mut self.write_buf, &indexer_request_payload).unwrap();
            let record: rdkafka::producer::BaseRecord<(), [u8], ()> =
                rdkafka::producer::BaseRecord::to(self.topics.indexer_request)
                    .payload(&self.write_buf);
            self.kafka_producer
                .send(record)
                .map_err(|(err, _)| err)
                .context(anyhow!(
                    "failed to send to topic {}",
                    self.topics.indexer_request
                ))?;
            self.write_buf.clear();

            if matches!(&indexer_request.receipt, Receipt::TAP(_)) {
                IndexerFeesProtobuf {
                    signer: self.tap_signer.to_vec(),
                    receiver: indexer_request.indexer.to_vec(),
                    fee_grt: indexer_request.receipt.grt_value() as f64 * 1e-18,
                }
                .encode(&mut self.write_buf)
                .unwrap();
                let record: rdkafka::producer::BaseRecord<(), [u8], ()> =
                    rdkafka::producer::BaseRecord::to(self.topics.indexer_fees)
                        .payload(&self.write_buf);
                self.kafka_producer
                    .send(record)
                    .map_err(|(err, _)| err)
                    .context(anyhow!(
                        "failed to send to topic {}",
                        self.topics.indexer_fees
                    ))?;
                self.write_buf.clear();
            }

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
                    rdkafka::producer::BaseRecord::to(self.topics.attestation)
                        .payload(&self.write_buf);
                self.kafka_producer
                    .send(record)
                    .map_err(|(err, _)| err)
                    .context(anyhow!(
                        "failed to send to topic {}",
                        self.topics.attestation
                    ))?;
                self.write_buf.clear();
            }
        }

        serde_json::to_writer(&mut self.write_buf, &client_request_payload).unwrap();
        let record: rdkafka::producer::BaseRecord<(), [u8], ()> =
            rdkafka::producer::BaseRecord::to(self.topics.client_request).payload(&self.write_buf);
        self.kafka_producer
            .send(record)
            .map_err(|(err, _)| err)
            .context(anyhow!(
                "failed to send to topic {}",
                self.topics.client_request
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

#[derive(Clone, PartialEq, prost::Message)]
pub struct IndexerFeesProtobuf {
    /// 20 bytes (address)
    #[prost(bytes, tag = "1")]
    signer: Vec<u8>,
    /// 20 bytes (address)
    #[prost(bytes, tag = "2")]
    receiver: Vec<u8>,
    #[prost(double, tag = "3")]
    fee_grt: f64,
}
