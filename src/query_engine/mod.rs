mod clock;
mod price_automation;
#[cfg(test)]
mod tests;
mod unattestable_errors;

use crate::{
    block_resolver::BlockResolver,
    fisherman_client::*,
    indexer_client::*,
    indexer_selection::{
        self, Context, IndexerError, IndexerPreferences, IndexerQuery, IndexerScore, Indexers,
        Receipt, SelectionError, UnresolvedBlock,
    },
    kafka_client::{ISAScoringError, ISAScoringSample, KafkaInterface},
    manifest_client::SubgraphInfo,
    query_stats,
};
pub use crate::{
    indexer_selection::{Indexing, UtilityConfig},
    prelude::*,
};
pub use graphql_client::Response;
use lazy_static::lazy_static;
pub use price_automation::{QueryBudgetFactors, VolumeEstimator};
use primitive_types::U256;
use prometheus;
use serde_json::value::RawValue;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use tokio::sync::Mutex;
use unattestable_errors::UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ClientQuery {
    pub id: QueryID,
    pub ray_id: String,
    pub api_key: Arc<APIKey>,
    pub subgraph: Ptr<SubgraphInfo>,
    pub query: Arc<String>,
    pub variables: Option<Arc<String>>,
}

#[derive(Clone, Debug)]
pub struct IndexerAttempt {
    pub score: IndexerScore,
    pub indexer: Address,
    pub allocation: Address,
    pub query: Arc<String>,
    pub receipt: Receipt,
    pub result: Result<IndexerResponse, IndexerError>,
    pub indexer_errors: String,
    pub duration: Duration,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
    pub is_subsidized: bool,
    pub user_id: i64,
    pub user_address: Address,
    pub queries_activated: bool,
    pub max_budget: Option<USD>,
    pub deployments: Vec<SubgraphDeploymentID>,
    pub subgraphs: Vec<(String, i32)>,
    pub domains: Vec<(String, i32)>,
    pub indexer_preferences: IndexerPreferences,
    pub usage: Arc<Mutex<VolumeEstimator>>,
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
    BlockBeforeMin,
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

#[derive(Clone)]
pub struct Config {
    pub indexer_selection_retry_limit: usize,
    pub budget_factors: QueryBudgetFactors,
}

#[derive(Clone)]
pub struct Inputs {
    pub indexers: Arc<Indexers>,
    pub current_deployments: Eventual<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>>,
    pub deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
}

pub struct InputWriters {
    pub indexer_inputs: indexer_selection::InputWriters,
    pub indexers: Arc<Indexers>,
    pub current_deployments: EventualWriter<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>>,
    pub deployment_indexers: EventualWriter<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
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

pub struct QueryEngine<I, K, F>
where
    I: IndexerInterface + Clone + Send,
    K: KafkaInterface + Send,
    F: FishermanInterface + Clone + Send,
{
    indexers: Arc<Indexers>,
    deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    block_resolvers: Arc<HashMap<String, BlockResolver>>,
    indexer_client: I,
    kafka_client: Arc<K>,
    fisherman_client: Option<Arc<F>>,
    query_stats: mpsc::UnboundedSender<query_stats::Msg>,
    config: Config,
}

impl<I, K, F> QueryEngine<I, K, F>
where
    I: IndexerInterface + Clone + Send + 'static,
    K: KafkaInterface + Send,
    F: FishermanInterface + Clone + Send + Sync + 'static,
{
    pub fn new(
        config: Config,
        indexer_client: I,
        kafka_client: Arc<K>,
        fisherman_client: Option<Arc<F>>,
        query_stats: mpsc::UnboundedSender<query_stats::Msg>,
        block_resolvers: Arc<HashMap<String, BlockResolver>>,
        inputs: Inputs,
    ) -> Self {
        Self {
            indexers: inputs.indexers,
            deployment_indexers: inputs.deployment_indexers,
            indexer_client,
            kafka_client,
            fisherman_client,
            block_resolvers,
            query_stats,
            config,
        }
    }

    pub async fn execute_query(
        &self,
        query: &ClientQuery,
    ) -> Result<(IndexerResponse, GRT), QueryEngineError> {
        let deployment_id = query.subgraph.deployment.ipfs_hash();
        let _timer = with_metric(
            &METRICS.queries.duration,
            &[&deployment_id, &query.api_key.key],
            |h| h.start_timer(),
        );
        let result = self.execute_deployment_query(query, &deployment_id).await;
        let result_counter = if let Ok(_) = result {
            &METRICS.queries.ok
        } else {
            &METRICS.queries.failed
        };
        with_metric(result_counter, &[&deployment_id, &query.api_key.key], |c| {
            c.inc()
        });
        result
    }

    #[tracing::instrument(skip(self, query, deployment_id))]
    async fn execute_deployment_query(
        &self,
        query: &ClientQuery,
        deployment_id: &str,
    ) -> Result<(IndexerResponse, GRT), QueryEngineError> {
        use QueryEngineError::*;
        let indexers = self
            .deployment_indexers
            .value_immediate()
            .and_then(|map| map.get(&query.subgraph.deployment).cloned())
            .unwrap_or_default();
        tracing::info!(deployment_indexers = indexers.len());
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
        let mut context = Context::new(
            &query_body,
            query_variables
                .as_ref()
                .map(|vars| vars.as_str())
                .unwrap_or("")
                .clone(),
        )
        .map_err(|_| MalformedQuery)?;

        let mut block_resolver = self
            .block_resolvers
            .get(&query.subgraph.network)
            .cloned()
            .ok_or(MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let freshness_requirements =
            Indexers::freshness_requirements(&mut context, &block_resolver).await?;

        // Reject queries for blocks before minimum start block of subgraph manifest.
        match freshness_requirements.minimum_block {
            Some(min_block) if min_block < query.subgraph.min_block => return Err(BlockBeforeMin),
            _ => (),
        };

        let query_count = context.operations.len().max(1) as u64;
        tracing::trace!(
            indexer_preferences = ?query.api_key.indexer_preferences,
            max_budget = ?query.api_key.max_budget,
        );
        // This has to run regardless even if we don't use the budget because
        // it updates the query volume estimate. This is important in the case
        // that the user switches back to automated volume discounting. Otherwise
        // it will look like there is a long period of inactivity which would increase
        // the price.
        let budget = query
            .api_key
            .usage
            .lock()
            .await
            .budget_for_queries(query_count, &self.config.budget_factors);
        let mut budget = USD::try_from(budget).unwrap();
        if let Some(max_budget) = query.api_key.max_budget {
            // Security: Consumers can and will set their budget to unreasonably high values. This
            // .min prevents the budget from being set beyond what it would be automatically. The reason
            // this is important is because sometimes queries are subsidized and we would be at-risk
            // to allow arbitrarily high values.
            budget = max_budget.min(budget * U256::from(10u8));
        }
        let budget: GRT = self
            .indexers
            .network_params
            .usd_to_grt(USD::try_from(budget).unwrap())
            .ok_or(SelectionError::MissingNetworkParams)?;

        let _ = self.query_stats.send(query_stats::Msg::BeginQuery {
            query: query.clone(),
            budget,
        });

        let utility_config = UtilityConfig::from_preferences(&query.api_key.indexer_preferences);

        // Used to track instances of the latest block getting unresolved by idexers. If this
        // happens enough, we will ignore the latest block since it may be uncled.
        let mut latest_unresolved: usize = 0;

        for _ in 0..self.config.indexer_selection_retry_limit {
            let selection_timer = with_metric(
                &METRICS.indexer_selection_duration,
                &[&deployment_id],
                |hist| hist.start_timer(),
            );

            tracing::info!(latest_block = ?block_resolver.latest_block());

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
                    &utility_config,
                    &query.subgraph.network,
                    &query.subgraph.deployment,
                    &indexers,
                    &mut context,
                    &block_resolver,
                    &freshness_requirements,
                    budget,
                )
                .await;
            selection_timer.map(|t| t.observe_duration());

            let (indexer_query, scoring_sample) = match selection_result {
                Ok(Some(indexer_query)) => indexer_query,
                Ok(None) => return Err(NoIndexerSelected),
                Err(err) => return Err(err.into()),
            };
            self.notify_isa_sample(
                &query,
                &indexer_query.indexing.indexer,
                &indexer_query.score,
                "Selected indexer score",
            );
            match scoring_sample.0 {
                Some((indexer, Ok(score))) => {
                    self.notify_isa_sample(&query, &indexer, &score, "ISA scoring sample")
                }
                Some((indexer, Err(err))) => {
                    self.notify_isa_err(&query, &indexer, err, "ISA scoring sample")
                }
                _ => (),
            };

            let indexer_id = indexer_query.indexing.indexer.to_string();
            self.observe_indexer_selection_metrics(deployment_id, &indexer_query);
            let indexer_query_start = Instant::now();
            let result = self.indexer_client.query_indexer(&indexer_query).await;
            let indexer_query_duration = Instant::now() - indexer_query_start;
            let indexer_errors = result
                .as_ref()
                .ok()
                .and_then(|response| {
                    serde_json::from_str::<Response<Box<RawValue>>>(&response.payload)
                        .ok()?
                        .errors
                })
                .unwrap_or_default()
                .into_iter()
                .map(|err| err.message)
                .collect::<Vec<String>>();
            let _ = self.query_stats.send(query_stats::Msg::AddIndexerAttempt {
                query_id: query.id,
                score: indexer_query.score.clone(),
                indexer: indexer_query.indexing.indexer.clone(),
                allocation: indexer_query.allocation.clone(),
                result: result.clone(),
                indexer_errors: indexer_errors.join(","),
                duration: indexer_query_duration,
            });
            with_metric(
                &METRICS.indexer_request.duration,
                &[deployment_id, &indexer_id],
                |hist| hist.observe(indexer_query_duration.as_secs_f64()),
            );
            let result = match result {
                Err(err) => Err(err),
                Ok(response) => self
                    .check_indexer_response(
                        &query,
                        &indexer_query,
                        &indexer_errors,
                        &response,
                        deployment_id,
                        &indexer_id,
                    )
                    .await
                    .map(|_| response),
            };
            let response = match result {
                Ok(response) => response,
                Err(err) => {
                    with_metric(
                        &METRICS.indexer_request.failed,
                        &[deployment_id, &indexer_id],
                        |counter| counter.inc(),
                    );
                    self.indexers
                        .observe_failed_query(
                            &indexer_query.indexing,
                            indexer_query_duration,
                            &indexer_query.receipt,
                            &err,
                        )
                        .await;
                    if let IndexerError::UnresolvedBlock = err {
                        self.indexers
                            .observe_indexing_behind(
                                &mut context,
                                &indexer_query.indexing,
                                &block_resolver,
                            )
                            .await;
                        // Skip 1 block for every 2 attempts where the indexer failed to resolve
                        // the block we consider to be latest. Our latest block may be uncled.
                        latest_unresolved += 1;
                        block_resolver.skip_latest(latest_unresolved / 2);
                    }
                    continue;
                }
            };
            if let Some(attestation) = &response.attestation {
                self.challenge_indexer_response(
                    indexer_query.indexing.clone(),
                    indexer_query.allocation,
                    indexer_query.query,
                    attestation.clone(),
                );
            }
            with_metric(
                &METRICS.indexer_request.ok,
                &[deployment_id, &indexer_id],
                |counter| counter.inc(),
            );
            self.indexers
                .observe_successful_query(
                    &indexer_query.indexing,
                    indexer_query_duration,
                    &indexer_query.receipt,
                )
                .await;

            return Ok((response, indexer_query.score.fee));
        }
        tracing::trace!("retry limit reached");
        Err(NoIndexerSelected)
    }

    async fn check_indexer_response(
        &self,
        query: &ClientQuery,
        indexer_query: &IndexerQuery,
        errors: &[String],
        response: &IndexerResponse,
        deployment_id: &str,
        indexer_id: &str,
    ) -> Result<(), IndexerError> {
        if errors.iter().any(|error| {
            error == "Failed to decode `block.hash` value: `no block with that hash found`"
        }) {
            return Err(IndexerError::UnresolvedBlock);
        }

        for error in errors {
            if UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS
                .iter()
                .any(|err| error.contains(err))
            {
                with_metric(
                    &METRICS.indexer_response_unattestable,
                    &[deployment_id, indexer_id],
                    |counter| counter.inc(),
                );
                tracing::info!("penalizing for unattestable error");
                self.indexers.penalize(&indexer_query.indexing, 35).await;
                return Err(IndexerError::UnattestableError);
            }
        }

        // TODO: This is a temporary hack to handle NonNullError being incorrectly categorized as
        // unattestable in graph-node.
        if response.attestation.is_none()
            && errors
                .iter()
                .any(|err| err.contains("Null value resolved for non-null field"))
        {
            return Ok(());
        }

        if !query.subgraph.features.is_empty() && response.attestation.is_none() {
            return Err(IndexerError::NoAttestation);
        }

        Ok(())
    }

    fn notify_isa_sample(
        &self,
        query: &ClientQuery,
        indexer: &Address,
        score: &IndexerScore,
        message: &str,
    ) {
        self.kafka_client
            .send(&ISAScoringSample::new(&query, &indexer, &score, message));
        // The following logs are required for data science.
        tracing::info!(
            ray_id = %query.ray_id,
            query_id = %query.id,
            deployment = %query.subgraph.deployment,
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
    }

    fn notify_isa_err(
        &self,
        query: &ClientQuery,
        indexer: &Address,
        err: SelectionError,
        message: &str,
    ) {
        self.kafka_client
            .send(&ISAScoringError::new(query, indexer, &err, message));
        // The following logs are required for data science.
        tracing::info!(
            ray_id = %query.ray_id,
            query_id = %query.id,
            deployment = %query.subgraph.deployment,
            %indexer,
            scoring_err = ?err,
            message,
        );
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

    fn challenge_indexer_response(
        &self,
        indexing: Indexing,
        allocation: Address,
        indexer_query: String,
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
