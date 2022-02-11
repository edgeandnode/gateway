#[cfg(test)]
mod tests;

use crate::redpanda::client::KafkaClient;

use crate::{
    block_resolver::BlockResolver,
    fisherman_client::*,
    indexer_client::*,
    indexer_selection::{
        self, Context, IndexerError, IndexerQuery, IndexerScore, Indexers, Receipt, SelectionError,
        UnresolvedBlock,
    },
    manifest_client::SubgraphInfo,
};
pub use crate::{
    indexer_selection::{Indexing, UtilityConfig},
    prelude::*,
};
pub use graphql_client::Response;
use im;
use lazy_static::lazy_static;
use prometheus;
use serde_json::value::RawValue;
use std::{
    collections::HashMap,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use uuid::Uuid;

use crate::redpanda::messages::{
    isa_scoring_error::ISAScoringError, isa_scoring_sample::ISAScoringSample,
};

use crate::redpanda::utils::MessageKind;
use futures_util::Future;

use futures_channel::oneshot::Canceled;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;
#[derive(Debug)]
pub struct Query {
    pub id: QueryID,
    pub ray_id: String,
    pub start_time: Instant,
    pub query: Rc<String>,
    pub variables: Rc<Option<String>>,
    pub api_key: Option<Arc<APIKey>>,
    pub subgraph: Option<Ptr<SubgraphInfo>>,
    pub indexer_attempts: Vec<IndexerAttempt>,
    pub kafka_client: Option<Arc<KafkaClient>>,
}

impl Query {
    pub fn new(ray_id: String, query: String, variables: Option<String>) -> Self {
        Self {
            id: QueryID::new(),
            start_time: Instant::now(),
            ray_id,
            query: Rc::new(query),
            variables: Rc::new(variables),
            api_key: None,
            subgraph: None,
            indexer_attempts: Vec::new(),
            kafka_client: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexerAttempt {
    pub score: IndexerScore,
    pub indexer: Address,
    pub allocation: Address,
    pub query: Arc<String>,
    pub receipt: Receipt,
    pub result: Result<IndexerResponse, IndexerError>,
    pub rejection: Option<String>,
    pub duration: Duration,
}

pub struct QueryID {
    pub local_id: u64,
}

impl QueryID {
    pub fn new() -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let local_id = COUNTER.fetch_add(1, MemoryOrdering::Relaxed) as u64;
        Self { local_id }
    }
}

impl fmt::Display for QueryID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        lazy_static! {
            static ref GATEWAY_ID: Uuid = Uuid::new_v4();
        }
        write!(f, "{}-{:x}", *GATEWAY_ID, self.local_id)
    }
}

impl fmt::Debug for QueryID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Clone, Debug, Default)]
pub struct APIKey {
    pub id: i64,
    pub key: String,
    pub user_id: i64,
    pub user_address: Address,
    pub queries_activated: bool,
    pub deployments: Vec<SubgraphDeploymentID>,
    pub subgraphs: Vec<(String, i32)>,
    pub domains: Vec<(String, i32)>,
}

#[derive(Debug)]
pub struct QueryResponse {
    pub query: IndexerQuery,
    pub response: IndexerResponse,
}

#[derive(Debug, PartialEq)]
pub enum QueryEngineError {
    NoIndexers,
    NoIndexerSelected,
    FeesTooHigh(usize),
    MalformedQuery,
    MissingBlock(UnresolvedBlock),
}

impl From<SelectionError> for QueryEngineError {
    fn from(from: SelectionError) -> Self {
        match from {
            SelectionError::MissingNetworkParams
            | SelectionError::BadIndexer(_)
            | SelectionError::NoAllocation(_) => Self::NoIndexerSelected,
            SelectionError::BadInput => Self::MalformedQuery,
            SelectionError::MissingBlock(unresolved) => Self::MissingBlock(unresolved),
            SelectionError::FeesTooHigh(count) => Self::FeesTooHigh(count),
        }
    }
}

enum RemoveIndexer {
    Yes,
    No,
}

#[derive(Clone)]
pub struct Config {
    pub indexer_selection_retry_limit: usize,
    pub utility: UtilityConfig,
    pub query_budget: GRT,
}

#[derive(Clone)]
pub struct Inputs {
    pub indexers: Arc<Indexers>,
    pub current_deployments: Eventual<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>>,
    pub deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, im::Vector<Address>>>>,
}

pub struct InputWriters {
    pub indexer_inputs: indexer_selection::InputWriters,
    pub indexers: Arc<Indexers>,
    pub current_deployments: EventualWriter<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>>,
    pub deployment_indexers:
        EventualWriter<Ptr<HashMap<SubgraphDeploymentID, im::Vector<Address>>>>,
}

impl Inputs {
    pub fn new() -> (InputWriters, Self) {
        let (indexer_input_writers, indexer_inputs) = Indexers::inputs();
        let indexers = Arc::new(Indexers::new(indexer_inputs));
        let (current_deployments_writer, current_deployments) = Eventual::new();
        let (deployment_indexers_writer, deployment_indexers) = Eventual::new();
        (
            InputWriters {
                indexer_inputs: indexer_input_writers,
                indexers: indexers.clone(),
                current_deployments: current_deployments_writer,
                deployment_indexers: deployment_indexers_writer,
            },
            Inputs {
                indexers,
                current_deployments,
                deployment_indexers,
            },
        )
    }
}

pub struct QueryEngine<I, F>
where
    I: IndexerInterface + Clone + Send,
    F: FishermanInterface + Clone + Send,
{
    indexers: Arc<Indexers>,
    deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, im::Vector<Address>>>>,
    block_resolvers: Arc<HashMap<String, BlockResolver>>,
    indexer_client: I,
    fisherman_client: Option<Arc<F>>,
    config: Config,
}

impl<I, F> QueryEngine<I, F>
where
    I: IndexerInterface + Clone + Send + 'static,
    F: FishermanInterface + Clone + Send + Sync + 'static,
{
    pub fn new(
        config: Config,
        indexer_client: I,
        fisherman_client: Option<Arc<F>>,
        block_resolvers: Arc<HashMap<String, BlockResolver>>,
        inputs: Inputs,
    ) -> Self {
        Self {
            indexers: inputs.indexers,
            deployment_indexers: inputs.deployment_indexers,
            indexer_client,
            fisherman_client,
            block_resolvers,
            config,
        }
    }

    pub async fn execute_query(&self, query: &mut Query) -> Result<(), QueryEngineError> {
        let api_key = query.api_key.as_ref().unwrap().key.clone();
        let deployment_id = query.subgraph.as_ref().unwrap().deployment.ipfs_hash();
        let _timer = with_metric(
            &METRICS.queries.duration,
            &[&deployment_id, &api_key],
            |h| h.start_timer(),
        );
        let result = self.execute_deployment_query(query, &deployment_id).await;
        let result_counter = if let Ok(_) = result {
            &METRICS.queries.ok
        } else {
            &METRICS.queries.failed
        };
        with_metric(result_counter, &[&deployment_id, &api_key], |c| c.inc());
        result
    }

    #[tracing::instrument(skip(self, query, deployment_id))]
    async fn execute_deployment_query(
        &self,
        query: &mut Query,
        deployment_id: &str,
    ) -> Result<(), QueryEngineError> {
        use QueryEngineError::*;
        let subgraph = query.subgraph.as_ref().unwrap().clone();
        let mut indexers = self
            .deployment_indexers
            .value_immediate()
            .and_then(|map| map.get(&subgraph.deployment).cloned())
            .unwrap_or_default();
        tracing::debug!(
            deployment = ?subgraph.deployment, deployment_indexers = indexers.len(),
        );
        if indexers.is_empty() {
            return Err(NoIndexers);
        }
        let _execution_timer = with_metric(
            &METRICS.query_execution_duration,
            &[&deployment_id],
            |hist| hist.start_timer(),
        );

        let query_body = query.query.clone();
        let query_variables = query.variables.clone();
        let mut context = Context::new(&query_body, query_variables.as_deref().unwrap_or_default())
            .map_err(|_| MalformedQuery)?;

        // let mut context = query.context().ok_or(MalformedQuery)?;
        let block_resolver = self
            .block_resolvers
            .get(&subgraph.network)
            .ok_or(MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let freshness_requirements =
            Indexers::freshness_requirements(&mut context, block_resolver).await?;

        for retry_count in 0..self.config.indexer_selection_retry_limit {
            let selection_timer = with_metric(
                &METRICS.indexer_selection_duration,
                &[&deployment_id],
                |hist| hist.start_timer(),
            );

            // Since we modify the context in-place, we need to reset the context to the state of
            // the original client query. This to avoid the following scenario:
            // 1. A client query has no block requirements set for some top-level operation
            // 2. The first indexer is selected, with some indexing status at block number `n`
            // 3. The query is made deterministic by setting the block requirement to the hash of
            //    block `n`
            // 4. Some condition requires us to retry this query on another indexer with an indexing
            //    status at a block less than `n`
            // 5. The same context is re-used, including the block requirement set to the hash of
            //    block `n`
            // 6. The indexer is seen as being behind and is unnecessarily penalized
            //
            // TODO: Avoid the additional cloning of the entire AST here, especially in the case
            // where retries are necessary. Only the top-level operation arguments need to be reset
            // to the state of the client query.
            let mut context = context.clone();

            let selection_result = self
                .indexers
                .select_indexer(
                    &self.config.utility,
                    &subgraph.network,
                    &subgraph.deployment,
                    &indexers,
                    &mut context,
                    &block_resolver,
                    &freshness_requirements,
                    self.config.query_budget,
                )
                .await;
            selection_timer.map(|t| t.observe_duration());

            match &selection_result {
                Ok(None) => tracing::info!(err = ?NoIndexerSelected),
                Err(err) => tracing::info!(?err),
                _ => (),
            };
            let (indexer_query, scoring_sample) = match selection_result {
                Ok(Some(indexer_query)) => indexer_query,
                Ok(None) => return Err(NoIndexerSelected),
                Err(err) => return Err(err.into()),
            };
            Self::log_indexer_score(
                &query,
                &indexer_query.indexing.indexer,
                &indexer_query.score,
                "Selected indexer score",
            );
            match scoring_sample.0 {
                Some((indexer, Ok(score))) => {
                    Self::log_indexer_score(&query, &indexer, &score, "ISA scoring sample").await
                }
                Some((indexer, Err(err))) => {
                    Self::log_indexer_score_err(&query, &indexer, err, "ISA scoring sample").await
                }
                _ => (),
            };
            let indexer = indexer_query.indexing.indexer;
            let result = self
                .execute_indexer_query(
                    query,
                    indexer_query,
                    deployment_id,
                    &mut context,
                    block_resolver,
                )
                .await;
            assert_eq!(retry_count + 1, query.indexer_attempts.len());
            match result {
                Ok(response) => return Ok(response),
                Err(RemoveIndexer::No) => (),
                Err(RemoveIndexer::Yes) => {
                    // TODO: There should be a penalty here, but the indexer should not be removed.
                    indexers.remove(indexers.iter().position(|i| i == &indexer).unwrap());
                }
            };
        }
        tracing::info!("retry limit reached");
        Err(NoIndexerSelected)
    }

    //https://stackoverflow.com/questions/70067281/why-doesnt-an-async-function-with-two-non-static-references-and-a-static-refere
    async fn log_indexer_score<'a, 'b>(
        query: &'a Query,
        indexer: &'b Address,
        score: &'b IndexerScore,
        message: &'static str,
    ) where
        'a: 'b,
    {
        let res = async move {
            tracing::info!(
                ray_id = %query.ray_id.clone(),
                query_id = %query.id,
                deployment = %query.subgraph.as_ref().unwrap().deployment,
                %indexer,
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
            //grab kafka
            let client = &query.kafka_client;

            let res = match client {
                Some(kafka_client) => {
                    let indexer_sample_msg = ISAScoringSample {
                        ray_id: query.ray_id.clone(),
                        query_id: query.id.local_id,
                        deployment: query.subgraph.as_ref().unwrap().deployment.to_vec(),
                        address: indexer.to_vec(),
                        fee: score.fee.to_string().to_owned(),
                        slashable: score.slashable.to_string().to_owned(),
                        utility: score.utility.into_inner(),
                        economic_security: score.utility_scores.economic_security,
                        price_efficiency: score.utility_scores.price_efficiency,
                        data_freshness: score.utility_scores.data_freshness,
                        performance: score.utility_scores.performance,
                        reputation: score.utility_scores.reputation,
                        sybil: *score.sybil,
                        blocks_behind: score.blocks_behind,
                        message: message.to_string(),
                    };
                    let delivery = kafka_client.send(
                        "gateway_isa_sample",
                        &indexer_sample_msg.write(MessageKind::AVRO),
                    );
                    delivery.unwrap().await;
                }
                None => (),
            };
            res
        };
    }

    async fn log_indexer_score_err<'a, 'b>(
        query: &'a Query,
        indexer: &'b Address,
        scoring_err: SelectionError,
        message: &'static str,
    ) where
        'a: 'b,
    {
        let res = async move {
            tracing::info!(
                ray_id = %query.ray_id.clone(),
                query_id = %query.id,
                deployment = %query.subgraph.as_ref().unwrap().deployment,
                %indexer,
                ?scoring_err,
                message,
            );
            let client = &query.kafka_client;

            let res = match client {
                Some(kafka_client) => {
                    let indexer_sample_error_msg = ISAScoringError {
                        ray_id: query.ray_id.clone(),
                        query_id: query.id.local_id,
                        deployment: query.subgraph.as_ref().unwrap().deployment.to_vec(),
                        indexer: indexer.to_vec(),
                        scoring_err: message.to_string(),
                    };
                    let delivery = kafka_client.send(
                        "gateway_isa_sample",
                        &indexer_sample_error_msg.write(MessageKind::AVRO),
                    );
                    delivery.unwrap().await;
                }
                None => (),
            };
            res
        };
    }

    async fn execute_indexer_query(
        &self,
        query: &mut Query,
        indexer_query: IndexerQuery,
        deployment_id: &str,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
    ) -> Result<(), RemoveIndexer> {
        let indexer_id = indexer_query.indexing.indexer.to_string();
        tracing::info!(indexer = %indexer_id);
        self.observe_indexer_selection_metrics(deployment_id, &indexer_query);
        let t0 = Instant::now();
        let result = self.indexer_client.query_indexer(&indexer_query).await;
        let query_duration = Instant::now() - t0;
        with_metric(
            &METRICS.indexer_request.duration,
            &[&deployment_id, &indexer_id],
            |hist| hist.observe(query_duration.as_secs_f64()),
        );
        query.indexer_attempts.push(IndexerAttempt {
            score: indexer_query.score,
            indexer: indexer_query.indexing.indexer,
            allocation: indexer_query.allocation,
            query: Arc::new(indexer_query.query),
            receipt: indexer_query.receipt,
            result: result,
            rejection: None,
            duration: query_duration,
        });
        let result = &query.indexer_attempts.last().unwrap();
        let response = match &result.result {
            Ok(response) => response,
            Err(err) => {
                self.indexers
                    .observe_failed_query(&indexer_query.indexing, &result.receipt, err.clone())
                    .await;
                with_metric(
                    &METRICS.indexer_request.failed,
                    &[&deployment_id, &indexer_id],
                    |counter| counter.inc(),
                );
                return Err(RemoveIndexer::Yes);
            }
        };
        with_metric(
            &METRICS.indexer_request.ok,
            &[&deployment_id, &indexer_id],
            |counter| counter.inc(),
        );

        let subgraph = query.subgraph.as_ref().unwrap();
        if !subgraph.features.is_empty() && response.attestation.is_none() {
            tracing::info!(indexer_response_err = "Attestable response has no attestation");
            self.indexers
                .observe_failed_query(
                    &indexer_query.indexing,
                    &result.receipt,
                    IndexerError::NoAttestation,
                )
                .await;
            return Err(RemoveIndexer::Yes);
        }

        if let Err(remove_indexer) = self
            .check_unattestable_responses(
                context,
                &block_resolver,
                &indexer_query.indexing,
                &result.receipt,
                &response,
            )
            .await
        {
            with_metric(
                &METRICS.indexer_response_unattestable,
                &[&deployment_id, &indexer_id],
                |counter| counter.inc(),
            );
            return Err(remove_indexer);
        }

        if let Some(attestation) = &response.attestation {
            self.challenge_indexer_response(
                indexer_query.indexing.clone(),
                result.allocation.clone(),
                result.query.clone(),
                attestation.clone(),
            );
        }

        self.indexers
            .observe_successful_query(&indexer_query.indexing, query_duration, &result.receipt)
            .await;
        Ok(())
    }

    fn observe_indexer_selection_metrics(&self, deployment: &str, selection: &IndexerQuery) {
        let metrics = &METRICS.indexer_selection;
        if let Ok(hist) = metrics
            .blocks_behind
            .get_metric_with_label_values(&[deployment])
        {
            hist.observe(selection.score.blocks_behind as f64);
        }
        if let Ok(hist) = metrics.fee.get_metric_with_label_values(&[deployment]) {
            hist.observe(selection.score.fee.as_f64());
        }
        if let Ok(counter) = metrics
            .indexer_selected
            .get_metric_with_label_values(&[deployment, &selection.indexing.indexer.to_string()])
        {
            counter.inc();
        }
        if let Ok(hist) = metrics
            .slashable_dollars
            .get_metric_with_label_values(&[deployment])
        {
            hist.observe(selection.score.slashable.as_f64());
        }
        if let Ok(hist) = metrics.utility.get_metric_with_label_values(&[deployment]) {
            hist.observe(*selection.score.utility);
        }
    }

    async fn check_unattestable_responses(
        &self,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        indexing: &Indexing,
        receipt: &Receipt,
        response: &IndexerResponse,
    ) -> Result<(), RemoveIndexer> {
        // Special-casing for a few known indexer errors; the block scope here
        // is just to separate the code neatly from the rest

        let parsed_response = serde_json::from_str::<Response<Box<RawValue>>>(&response.payload)
            .map_err(|_| RemoveIndexer::Yes)?;

        if indexer_response_has_error(
            &parsed_response,
            "Failed to decode `block.hash` value: `no block with that hash found`",
        ) {
            tracing::info!(indexer_response_err = "indexing behind");
            self.indexers
                .observe_indexing_behind(context, indexing, block_resolver)
                .await;
            return Err(RemoveIndexer::No);
        }

        if indexer_response_has_error(&parsed_response, "panic processing query") {
            tracing::info!(indexer_response_err = "panic processing query");
            self.indexers
                .observe_failed_query(indexing, receipt, IndexerError::NondeterministicResponse)
                .await;
            return Err(RemoveIndexer::Yes);
        }

        Ok(())
    }

    fn challenge_indexer_response(
        &self,
        indexing: Indexing,
        allocation: Address,
        indexer_query: Arc<String>,
        attestation: Attestation,
    ) {
        let fisherman = match &self.fisherman_client {
            Some(fisherman) => fisherman.clone(),
            None => return,
        };
        let indexers = self.indexers.clone();
        tokio::spawn(async move {
            let outcome = fisherman
                .challenge(&indexing.indexer, &allocation, &indexer_query, &attestation)
                .await;
            tracing::trace!(?outcome);
            let penalty = match outcome {
                ChallengeOutcome::Unknown | ChallengeOutcome::AgreeWithTrustedIndexer => 0,
                ChallengeOutcome::DisagreeWithUntrustedIndexer => 10,
                ChallengeOutcome::DisagreeWithTrustedIndexer => 35,
                ChallengeOutcome::FailedToProvideAttestation => 40,
            };
            if penalty > 0 {
                tracing::info!(?outcome, "penalizing for challenge outcome");
                indexers.penalize(&indexing, penalty).await;
            }
        });
    }
}

struct Metrics {
    indexer_request: ResponseMetricVecs,
    indexer_response_unattestable: prometheus::IntCounterVec,
    indexer_selection: IndexerSelectionMetrics,
    indexer_selection_duration: prometheus::HistogramVec,
    queries: ResponseMetricVecs,
    query_execution_duration: prometheus::HistogramVec,
}

struct IndexerSelectionMetrics {
    blocks_behind: prometheus::HistogramVec,
    fee: prometheus::HistogramVec,
    indexer_selected: prometheus::IntCounterVec,
    slashable_dollars: prometheus::HistogramVec,
    utility: prometheus::HistogramVec,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
}

impl Metrics {
    fn new() -> Self {
        Self {
            indexer_request: ResponseMetricVecs::new(
                "query_engine_indexer_request",
                "indexer responses",
                &["deployment", "indexer"],
            ),
            indexer_response_unattestable: prometheus::register_int_counter_vec!(
                "query_engine_indexer_response_unattestable",
                "Number of unattestable indexer responses",
                &["deployment", "indexer"]
            )
            .unwrap(),
            indexer_selection: IndexerSelectionMetrics {
                blocks_behind: prometheus::register_histogram_vec!(
                    "indexer_selection_blocks_behind",
                    "Number of blocks that indexers are behind",
                    &["deployment"]
                )
                .unwrap(),
                fee: prometheus::register_histogram_vec!(
                    "indexer_selection_fee",
                    "Query fee amount based on cost models",
                    &["deployment"]
                )
                .unwrap(),
                indexer_selected: prometheus::register_int_counter_vec!(
                    "indexer_selection_indexer_selected",
                    "Number of times an indexer was selected",
                    &["deployment", "indexer"]
                )
                .unwrap(),
                slashable_dollars: prometheus::register_histogram_vec!(
                    "indexer_selection_slashable_dollars",
                    "Slashable dollars of selected indexers",
                    &["deployment"]
                )
                .unwrap(),
                utility: prometheus::register_histogram_vec!(
                    "indexer_selection_utility",
                    "Combined overall utility",
                    &["deployment"]
                )
                .unwrap(),
            },
            indexer_selection_duration: prometheus::register_histogram_vec!(
                "query_engine_indexer_selection_duration",
                "Duration of selecting an indexer for a query",
                &["deployment"]
            )
            .unwrap(),
            queries: ResponseMetricVecs::new(
                "gateway_queries",
                "processing queries",
                &["deployment", "apiKey"],
            ),
            query_execution_duration: prometheus::register_histogram_vec!(
                "query_engine_query_execution_duration",
                "Duration of executing the query for a deployment",
                &["deployment"]
            )
            .unwrap(),
        }
    }
}

/// Returns true if the GraphQL response includes at least one error whose
/// message begins with the given message.
fn indexer_response_has_error(response: &Response<Box<RawValue>>, msg: &'static str) -> bool {
    response
        .errors
        .as_ref()
        .map(|errs| errs.iter().any(|err| err.message.starts_with(msg)))
        .unwrap_or(false)
}
