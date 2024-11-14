use anyhow::{anyhow, Context};
use ordered_float::NotNan;
use prost::Message;
use thegraph_core::{Address, AllocationId, DeploymentId, IndexerId};
use tokio::sync::mpsc;
use toolshed::concat_bytes;

use crate::{errors, indexer_client::IndexerResponse, receipts::Receipt};

pub struct ClientRequest {
    pub id: String,
    pub response_time_ms: u16,
    pub result: Result<(), errors::Error>,
    pub api_key: String,
    pub user: String,
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
    pub topics: Topics,
    pub write_buf: Vec<u8>,
    pub kafka_producer: rdkafka::producer::ThreadedProducer<
        rdkafka::producer::DefaultProducerContext,
        rdkafka::producer::NoCustomPartitioner,
    >,
}

pub struct Topics {
    pub queries: &'static str,
    pub attestations: &'static str,
}

impl Reporter {
    pub fn create(
        tap_signer: Address,
        graph_env: String,
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
        let indexer_queries = client_request
            .indexer_requests
            .iter()
            .map(|indexer_request| IndexerQueryProtobuf {
                indexer: indexer_request.indexer.to_vec(),
                deployment: indexer_request.deployment.as_ref().to_vec(),
                allocation: indexer_request.receipt.allocation().to_vec(),
                indexed_chain: indexer_request.subgraph_chain.clone(),
                url: indexer_request.url.clone(),
                fee_grt: indexer_request.receipt.grt_value() as f64 * 1e-18,
                response_time_ms: indexer_request.response_time_ms as u32,
                seconds_behind: indexer_request.seconds_behind,
                result: indexer_request
                    .result
                    .as_ref()
                    .map(|_| "success".to_string())
                    .unwrap_or_else(|err| err.to_string()),
                indexer_errors: indexer_request
                    .result
                    .as_ref()
                    .map(|r| {
                        r.errors
                            .iter()
                            .map(|err| err.as_str())
                            .collect::<Vec<&str>>()
                            .join("; ")
                    })
                    .unwrap_or_default(),
                blocks_behind: indexer_request.blocks_behind,
                legacy_scalar: matches!(&indexer_request.receipt, Receipt::Legacy(_, _)),
            })
            .collect();

        let total_fees_grt: f64 = client_request
            .indexer_requests
            .iter()
            .map(|i| i.receipt.grt_value() as f64 * 1e-18)
            .sum();
        let total_fees_usd: f64 = total_fees_grt / *client_request.grt_per_usd;

        let client_query_msg = ClientQueryProtobuf {
            gateway_id: self.graph_env.clone(),
            receipt_signer: self.tap_signer.to_vec(),
            query_id: client_request.id,
            api_key: client_request.api_key,
            user_id: client_request.user,
            result: client_request
                .result
                .map(|()| "success".to_string())
                .unwrap_or_else(|err| err.to_string()),
            response_time_ms: client_request.response_time_ms as u32,
            request_bytes: client_request.request_bytes,
            response_bytes: client_request.response_bytes,
            total_fees_usd,
            indexer_queries,
        };

        client_query_msg.encode(&mut self.write_buf).unwrap();
        let record: rdkafka::producer::BaseRecord<(), [u8], ()> =
            rdkafka::producer::BaseRecord::to(self.topics.queries).payload(&self.write_buf);
        self.kafka_producer
            .send(record)
            .map_err(|(err, _)| err)
            .context(anyhow!("failed to send to topic {}", self.topics.queries))?;
        self.write_buf.clear();

        for indexer_request in client_request.indexer_requests {
            if let Some((response, attestation)) = indexer_request
                .result
                .ok()
                .and_then(|r| Some((r.response, r.attestation?)))
            {
                const MAX_PAYLOAD_BYTES: usize = 10_000;
                AttestationProtobuf {
                    request: Some(indexer_request.request).filter(|r| r.len() <= MAX_PAYLOAD_BYTES),
                    response: Some(response).filter(|r| r.len() <= MAX_PAYLOAD_BYTES),
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
                    rdkafka::producer::BaseRecord::to(self.topics.attestations)
                        .payload(&self.write_buf);
                self.kafka_producer
                    .send(record)
                    .map_err(|(err, _)| err)
                    .context(anyhow!(
                        "failed to send to topic {}",
                        self.topics.attestations
                    ))?;
                self.write_buf.clear();
            }
        }

        Ok(())
    }
}

#[derive(prost::Message)]
pub struct ClientQueryProtobuf {
    #[prost(string, tag = "1")]
    gateway_id: String,
    // 20 bytes
    #[prost(bytes, tag = "2")]
    receipt_signer: Vec<u8>,
    #[prost(string, tag = "3")]
    query_id: String,
    #[prost(string, tag = "4")]
    api_key: String,
    #[prost(string, tag = "11")]
    user_id: String,
    #[prost(string, tag = "5")]
    result: String,
    #[prost(uint32, tag = "6")]
    response_time_ms: u32,
    #[prost(uint32, tag = "7")]
    request_bytes: u32,
    #[prost(uint32, optional, tag = "8")]
    response_bytes: Option<u32>,
    #[prost(double, tag = "9")]
    total_fees_usd: f64,
    #[prost(message, repeated, tag = "10")]
    indexer_queries: Vec<IndexerQueryProtobuf>,
}

#[derive(prost::Message)]
pub struct IndexerQueryProtobuf {
    /// 20 bytes
    #[prost(bytes, tag = "1")]
    indexer: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "2")]
    deployment: Vec<u8>,
    /// 20 bytes
    #[prost(bytes, tag = "3")]
    allocation: Vec<u8>,
    #[prost(string, tag = "4")]
    indexed_chain: String,
    #[prost(string, tag = "5")]
    url: String,
    #[prost(double, tag = "6")]
    fee_grt: f64,
    #[prost(uint32, tag = "7")]
    response_time_ms: u32,
    #[prost(uint32, tag = "8")]
    seconds_behind: u32,
    #[prost(string, tag = "9")]
    result: String,
    #[prost(string, tag = "10")]
    indexer_errors: String,
    #[prost(uint64, tag = "11")]
    blocks_behind: u64,
    #[prost(bool, tag = "12")]
    legacy_scalar: bool,
}

#[derive(prost::Message)]
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
