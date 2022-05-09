use crate::{
    indexer_client::IndexerResponse,
    indexer_selection::{IndexerError, IndexerScore},
    kafka_client::{ClientQueryResult, IndexerAttempt, KafkaInterface},
    prelude::*,
    query_engine::{ClientQuery, QueryID},
};
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::spawn;

pub enum Msg {
    BeginQuery {
        query: ClientQuery,
        budget: GRT,
    },
    AddIndexerAttempt {
        query_id: QueryID,
        indexer: Address,
        score: IndexerScore,
        allocation: Address,
        result: Result<IndexerResponse, IndexerError>,
        indexer_errors: String,
        duration: Duration,
    },
    EndQuery {
        query_id: QueryID,
        result: Result<String, String>,
    },
}

pub struct Actor<K: KafkaInterface> {
    queries: HashMap<QueryID, Query>,
    kafka: Arc<K>,
}

struct Query {
    pub query: ClientQuery,
    pub budget: GRT,
    pub start_time: Instant,
    pub indexer_attempts: Vec<IndexerAttempt>,
}

impl<K: KafkaInterface + Send + Sync + 'static> Actor<K> {
    pub fn create(kafka: Arc<K>) -> mpsc::UnboundedSender<Msg> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut actor = Self {
            queries: HashMap::new(),
            kafka,
        };
        spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    Msg::BeginQuery { query, budget } => {
                        actor.queries.insert(
                            query.id.clone(),
                            Query {
                                query,
                                budget,
                                start_time: Instant::now(),
                                indexer_attempts: Vec::new(),
                            },
                        );
                    }
                    Msg::AddIndexerAttempt {
                        query_id,
                        indexer,
                        score,
                        allocation,
                        result,
                        indexer_errors,
                        duration,
                    } => {
                        let info = match actor.queries.get_mut(&query_id) {
                            Some(info) => info,
                            None => {
                                tracing::error!(%query_id, "Missing query_id");
                                continue;
                            }
                        };
                        let status = match &result {
                            Ok(response) => response.status.to_string(),
                            Err(err) => format!("{:?}", err),
                        };
                        let status_code = {
                            let (prefix, data) = match &result {
                                // prefix 0x0, followed by the HTTP status code
                                Ok(response) => (0x0, (response.status as u32).to_be()),
                                Err(IndexerError::NoAttestation) => (0x1, 0x0),
                                Err(IndexerError::UnattestableError) => (0x2, 0x0),
                                Err(IndexerError::Timeout) => (0x3, 0x0),
                                Err(IndexerError::UnresolvedBlock) => (0x5, 0x0),
                                // prefix 0x6, followed by a 28-bit hash of the error message
                                Err(IndexerError::Other(msg)) => (0x6, sip24_hash(&msg) as u32),
                            };
                            (prefix << 28) | (data & (u32::MAX >> 4))
                        };
                        info.indexer_attempts.push(IndexerAttempt {
                            query_id: info.query.id.to_string(),
                            ray_id: info.query.ray_id.clone(),
                            api_key: info.query.api_key.key.clone(),
                            deployment: info.query.subgraph.deployment.to_string(),
                            indexer: indexer.to_string(),
                            url: score.url.as_ref().clone(),
                            allocation: allocation.to_string(),
                            fee: score.fee.as_f64(),
                            utility: *score.utility,
                            blocks_behind: score.blocks_behind,
                            indexer_errors,
                            response_time_ms: duration.as_millis() as u32,
                            status,
                            status_code,
                            timestamp: Self::timestamp(),
                        });
                    }
                    Msg::EndQuery { query_id, result } => {
                        let info = match actor.queries.remove(&query_id) {
                            Some(query) => query,
                            None => continue,
                        };
                        let (status, status_code) = match result {
                            Ok(status) => (status, 0),
                            Err(status) => {
                                let code = sip24_hash(&status) as u32 | 0x1;
                                (status, code)
                            }
                        };

                        // The following logs are required for data science.
                        tracing::info!(
                            query_id = %info.query.id,
                            ray_id = %info.query.ray_id,
                            deployment = %info.query.subgraph.deployment,
                            network = %info.query.subgraph.network,
                            api_key = %info.query.api_key.key,
                            query = %info.query.query,
                            variables = %info.query.variables.as_ref().map(|vars| vars.as_str()).unwrap_or(""),
                            budget = %info.budget,
                            response_time_ms = (Instant::now() - info.start_time).as_millis() as u32,
                            %status,
                            status_code,
                            "Client query result",
                        );
                        for (attempt_index, attempt) in info.indexer_attempts.iter().enumerate() {
                            tracing::info!(
                                query_id = %attempt.query_id,
                                ray_id = %attempt.ray_id,
                                api_key = %attempt.api_key,
                                deployment = %attempt.deployment,
                                attempt_index,
                                indexer = %attempt.indexer,
                                url = %attempt.url,
                                allocation = %attempt.allocation,
                                fee = %attempt.fee,
                                utility = attempt.utility,
                                blocks_behind = attempt.blocks_behind,
                                indexer_errors = %attempt.indexer_errors,
                                response_time_ms = attempt.response_time_ms,
                                status = %attempt.status,
                                status_code = attempt.status_code,
                                "Indexer attempt",
                            );
                        }

                        actor.kafka.send(&ClientQueryResult {
                            timestamp: Self::timestamp(),
                            query_id: info.query.id.to_string(),
                            ray_id: info.query.ray_id.clone(),
                            api_key: info.query.api_key.key.clone(),
                            network: info.query.subgraph.network.to_string(),
                            deployment: info.query.subgraph.deployment.to_string(),
                            budget: info.budget.to_string(),
                            response_time_ms: (Instant::now() - info.start_time).as_millis() as u32,
                            status: status.clone(),
                            status_code,
                        });
                        for attempt in &info.indexer_attempts {
                            actor.kafka.send(attempt);
                        }
                    }
                }
            }
        });
        tx
    }

    fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[cfg(test)]
pub fn test_create() -> (
    Arc<tokio::sync::Mutex<Vec<Msg>>>,
    mpsc::UnboundedSender<Msg>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let stats = Arc::<tokio::sync::Mutex<Vec<Msg>>>::default();
    spawn({
        let stats = stats.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                stats.lock().await.push(msg);
            }
        }
    });
    (stats, tx)
}
