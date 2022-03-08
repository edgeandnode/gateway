use crate::{
    indexer_client::IndexerResponse,
    indexer_selection::{IndexerError, IndexerScore, SelectionError},
    prelude::*,
    query_engine::Query,
};
use lazy_static::lazy_static;
use rdkafka::{
    config::ClientConfig,
    error::KafkaResult,
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
};
use serde::Serialize;
use siphasher::sip::SipHasher24;
use std::hash::{Hash as _, Hasher as _};

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
                tracing::error!(%kafka_producer_err);
                METRICS.failed_sends.inc();
            }
        }
    }
}

#[derive(Serialize)]
pub struct ClientQueryResult {
    pub ray_id: String,
    pub query_id: String,
    pub api_key: String,
    pub deployment: String,
    pub network: String,
    pub query: String,
    pub variables: String,
    pub response_time_ms: u32,
    pub budget: String,
    pub status: String,
    pub status_code: u32,
    pub indexer_attempts: Vec<IndexerAttempt>,
}

#[derive(Serialize)]
pub struct IndexerAttempt {
    pub indexer: String,
    pub url: String,
    pub allocation: String,
    pub fee: f64,
    pub utility: f64,
    pub blocks_behind: u64,
    pub response_time_ms: u32,
    pub status: String,
    pub status_code: u32,
}

impl ClientQueryResult {
    pub fn new(query: &Query, result: Result<String, String>) -> Self {
        let api_key = &query.api_key.as_ref().unwrap().key;
        let subgraph = query.subgraph.as_ref().unwrap();
        let deployment = subgraph.deployment.to_string();
        let network = &query.subgraph.as_ref().unwrap().network;
        let variables = query.variables.as_deref().unwrap_or("");
        let response_time_ms = (Instant::now() - query.start_time).as_millis() as u32;
        let budget = query
            .budget
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let (status, status_code) = match &result {
            Ok(status) => (status, 0),
            Err(status) => (status, Self::hash_msg(status) | 0x1),
        };
        tracing::info!(
            ray_id = %query.ray_id,
            query_id = %query.id,
            %deployment,
            %network,
            %api_key,
            query = %query.query,
            variables = %variables,
            budget = %budget,
            response_time_ms,
            %status,
            status_code,
            "Client query result",
        );
        let indexer_attempts = query
            .indexer_attempts
            .iter()
            .map(|attempt| IndexerAttempt {
                indexer: attempt.indexer.to_string(),
                url: attempt.score.url.to_string(),
                allocation: attempt.allocation.to_string(),
                fee: attempt.score.fee.as_f64(),
                utility: *attempt.score.utility,
                blocks_behind: attempt.score.blocks_behind,
                response_time_ms: attempt.duration.as_millis() as u32,
                status: match &attempt.result {
                    Ok(response) => response.status.to_string(),
                    Err(err) => format!("{:?}", err),
                },
                status_code: Self::encode_indexer_attempt_status(&attempt.result),
            })
            .collect::<Vec<IndexerAttempt>>();
        for (attempt_index, attempt) in indexer_attempts.iter().enumerate() {
            tracing::info!(
                ray_id = %query.ray_id,
                query_id = %query.id,
                %api_key,
                %deployment,
                attempt_index,
                indexer = %attempt.indexer,
                url = %attempt.url,
                allocation = %attempt.allocation,
                fee = attempt.fee,
                utility = attempt.utility,
                blocks_behind = attempt.blocks_behind,
                response_time_ms = attempt.response_time_ms,
                %status,
                status_code,
                "Indexer attempt",
            );
        }

        Self {
            ray_id: query.ray_id.clone(),
            query_id: query.id.to_string(),
            api_key: api_key.clone(),
            deployment: deployment,
            network: network.clone(),
            query: query.query.to_string(),
            variables: variables.to_string(),
            response_time_ms,
            budget,
            status: status.clone(),
            status_code,
            indexer_attempts,
        }
    }

    // 32-bit status, encoded as `| 31:28 prefix | 27:0 data |` (big-endian)
    fn encode_indexer_attempt_status(result: &Result<IndexerResponse, IndexerError>) -> u32 {
        let (prefix, data) = match result {
            // prefix 0x0, followed by the HTTP status code
            Ok(response) => (0x0, (response.status as u32).to_be()),
            Err(IndexerError::NoAttestation) => (0x1, 0x0),
            Err(IndexerError::Panic) => (0x2, 0x0),
            Err(IndexerError::Timeout) => (0x3, 0x0),
            Err(IndexerError::UnexpectedPayload) => (0x4, 0x0),
            Err(IndexerError::UnresolvedBlock) => (0x5, 0x0),
            // prefix 0x6, followed by a 28-bit hash of the error message
            Err(IndexerError::Other(msg)) => (0x6, Self::hash_msg(&msg)),
        };
        (prefix << 28) | (data & (u32::MAX >> 4))
    }

    fn hash_msg(msg: &String) -> u32 {
        let mut hasher = SipHasher24::default();
        msg.hash(&mut hasher);
        hasher.finish() as u32
    }
}

impl Msg for ClientQueryResult {
    const TOPIC: &'static str = "gateway_client_query_results";
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

impl ISAScoringSample {
    pub fn new(query: &Query, indexer: &Address, score: &IndexerScore, message: &str) -> Self {
        tracing::info!(
            ray_id = %query.ray_id,
            query_id = %query.id,
            deployment = %query.subgraph.as_ref().unwrap().deployment,
            %indexer,
            url = %score.url,
            fee = %score.fee,
            slashable = %score.slashable,
            utility = *score.utility,
            economic_security = score.utility_scores.economic_security,
            price_efficiency = score.utility_scores.price_efficiency,
            data_freshness = score.utility_scores.data_freshness,
            performance = score.utility_scores.performance,
            reputation = score.utility_scores.reputation,
            sybil = *score.sybil,
            blocks_behind = score.blocks_behind,
            message,
        );
        Self {
            ray_id: query.ray_id.clone(),
            query_id: query.id.local_id,
            deployment: query.subgraph.as_ref().unwrap().deployment.to_string(),
            address: indexer.to_string(),
            fee: score.fee.to_string(),
            slashable: score.slashable.to_string(),
            utility: *score.utility,
            economic_security: score.utility_scores.economic_security,
            price_efficiency: score.utility_scores.price_efficiency,
            data_freshness: score.utility_scores.data_freshness,
            performance: score.utility_scores.performance,
            reputation: score.utility_scores.reputation,
            sybil: *score.sybil,
            blocks_behind: score.blocks_behind,
            url: score.url.to_string(),
            message: message.to_string(),
        }
    }
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

impl ISAScoringError {
    pub fn new(
        query: &Query,
        indexer: &Address,
        scoring_err: SelectionError,
        message: &str,
    ) -> Self {
        tracing::info!(
            ray_id = %query.ray_id.clone(),
            query_id = %query.id,
            deployment = %query.subgraph.as_ref().unwrap().deployment,
            %indexer,
            ?scoring_err,
            message,
        );
        Self {
            ray_id: query.ray_id.clone(),
            query_id: query.id.local_id,
            deployment: query.subgraph.as_ref().unwrap().deployment.to_string(),
            indexer: indexer.to_string(),
            scoring_err: message.to_string(),
        }
    }
}

impl Msg for ISAScoringError {
    const TOPIC: &'static str = "gateway_isa_errors";
}

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
}

struct Metrics {
    pub failed_sends: prometheus::IntCounter,
}

impl Metrics {
    fn new() -> Self {
        Self {
            failed_sends: prometheus::register_int_counter!(
                "gateway_kafka_failed_sends",
                "Failed message sends to Kafka topics",
            )
            .unwrap(),
        }
    }
}
