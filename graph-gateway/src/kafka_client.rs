use crate::indexer_client::{IndexerError, ResponsePayload};
use prelude::*;
use rdkafka::{
    config::ClientConfig,
    error::KafkaResult,
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
};
use serde::Serialize;
use std::time::SystemTime;

pub trait Msg: Serialize {
    const TOPIC: &'static str;
}

pub struct KafkaClient {
    producer: ThreadedProducer<DefaultProducerContext>,
}

impl KafkaClient {
    pub fn new(config: &ClientConfig) -> KafkaResult<KafkaClient> {
        let producer = config.create_with_context(DefaultProducerContext)?;
        Ok(KafkaClient { producer })
    }
    pub fn send<M: Msg>(&self, msg: &M) {
        let payload = serde_json::to_vec(msg).unwrap();
        let record = BaseRecord::<'_, (), [u8]>::to(M::TOPIC).payload(&payload);
        match self.producer.send(record) {
            Ok(()) => (),
            Err((kafka_producer_err, _)) => tracing::error!(%kafka_producer_err, topic=%M::TOPIC),
        };
    }
}

#[derive(Default, Serialize)]
pub struct ClientQueryResult {
    pub query_id: String,
    pub ray_id: String,
    pub graph_env: String,
    pub timestamp: u64,
    pub api_key: String,
    pub deployment: String,
    pub network: String,
    pub response_time_ms: u32,
    pub budget: String,
    pub budget_float: f32,
    pub query_count: u64,
    pub fee: f32,
    pub status: String,
    pub status_code: u32,
}

impl Msg for ClientQueryResult {
    const TOPIC: &'static str = "gateway_client_query_results";
}

#[derive(Clone, Default, Serialize)]
pub struct IndexerAttempt {
    pub query_id: String,
    pub ray_id: String,
    pub graph_env: String,
    pub api_key: String,
    pub deployment: String,
    pub network: String,
    pub indexer: String,
    pub url: String,
    pub allocation: String,
    pub fee: f64,
    pub utility: f64,
    pub blocks_behind: u64,
    pub indexer_errors: String,
    pub response_time_ms: u32,
    pub status: String,
    pub status_code: u32,
    pub timestamp: u64,
}

impl Msg for IndexerAttempt {
    const TOPIC: &'static str = "gateway_indexer_attempts";
}

// 32-bit status, encoded as `| 31:28 prefix | 27:0 data |` (big-endian)
pub fn indexer_attempt_status_code(result: &Result<ResponsePayload, IndexerError>) -> u32 {
    let (prefix, data) = match &result {
        // prefix 0x0, followed by the HTTP status code
        Ok(_) => (0x0, 200_u32.to_be()),
        Err(IndexerError::NoAttestation) => (0x1, 0x0),
        Err(IndexerError::UnattestableError) => (0x2, 0x0),
        Err(IndexerError::Timeout) => (0x3, 0x0),
        Err(IndexerError::UnexpectedPayload) => (0x4, 0x0),
        Err(IndexerError::UnresolvedBlock) => (0x5, 0x0),
        Err(IndexerError::NoAllocation) => (0x7, 0x0),
        // prefix 0x6, followed by a 28-bit hash of the error message
        Err(IndexerError::Other(msg)) => (0x6, sip24_hash(&msg) as u32),
    };
    (prefix << 28) | (data & (u32::MAX >> 4))
}

pub fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
