use crate::prelude::*;
use rdkafka::{
    config::ClientConfig,
    error::KafkaResult,
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
};
use serde::Serialize;

pub trait Msg: Serialize {
    const TOPIC: &'static str;
}

pub trait KafkaInterface {
    fn send<M: Msg>(&self, msg: &M);
}

pub struct KafkaClient {
    producer: ThreadedProducer<DefaultProducerContext>,
}

impl KafkaClient {
    pub fn new(config: &ClientConfig) -> KafkaResult<KafkaClient> {
        let producer = config.create_with_context(DefaultProducerContext)?;
        Ok(KafkaClient { producer })
    }
}

impl KafkaInterface for KafkaClient {
    fn send<M: Msg>(&self, msg: &M) {
        let payload = serde_json::to_vec(msg).unwrap();
        let record = BaseRecord::<'_, (), [u8]>::to(M::TOPIC).payload(&payload);
        match self.producer.send(record) {
            Ok(()) => (),
            Err((kafka_producer_err, _)) => {
                tracing::error!(%kafka_producer_err)
            }
        }
    }
}

#[derive(Serialize)]
pub struct ClientQueryResult {
    pub ray_id: String,
    pub query_id: u64,
    pub deployment: String,
    pub network: String,
    pub api_key: String,
    pub query: String,
    pub response_time: u32,
    pub variables: String,
    pub budget: String,
    pub status: String,
}

impl Msg for ClientQueryResult {
    const TOPIC: &'static str = "gateway_client_query_results";
}

#[derive(Serialize)]
pub struct IndexerAttempt {
    pub ray_id: String,
    pub query_id: u64,
    pub attempt_index: usize,
    pub indexer: String,
    pub allocation: String,
    pub fee: String,
    pub utility: f64,
    pub blocks_behind: u64,
    pub response_time_ms: u32,
    pub status: String,
    pub status_code: u32,
}

impl Msg for IndexerAttempt {
    const TOPIC: &'static str = "gateway_indexer_attempts";
}

#[derive(Serialize)]
pub struct ISAScoringSample {
    pub ray_id: String,
    pub query_id: u64,
    pub deployment: String,
    pub address: String,
    pub fee: String,
    pub slashable: String,
    pub utility: f64,
    pub economic_security: f64,
    pub price_efficiency: f64,
    pub data_freshness: f64,
    pub performance: f64,
    pub reputation: f64,
    pub sybil: f64,
    pub blocks_behind: u64,
    pub url: String,
    pub message: String,
}

impl Msg for ISAScoringSample {
    const TOPIC: &'static str = "gateway_isa_samples";
}

#[derive(Serialize)]
pub struct ISAScoringError {
    pub ray_id: String,
    pub query_id: u64,
    pub deployment: String,
    pub indexer: String,
    pub scoring_err: String,
}

impl Msg for ISAScoringError {
    const TOPIC: &'static str = "gateway_isa_errors";
}
