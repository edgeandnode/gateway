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
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use tokio::sync::Mutex;
use unattestable_errors::UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS;
use uuid::Uuid;

#[derive(Debug)]
pub struct Query {
    pub id: QueryID,
    pub ray_id: String,
    pub start_time: Instant,
    pub query: Rc<String>,
    pub variables: Rc<Option<String>>,
    pub api_key: Option<Arc<APIKey>>,
    pub subgraph: Option<Ptr<SubgraphInfo>>,
    pub budget: Option<GRT>,
    pub indexer_attempts: Vec<IndexerAttempt>,
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
            budget: None,
            indexer_attempts: Vec::new(),
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
    pub indexer_errors: String,
    pub duration: Duration,
}

impl IndexerAttempt {
    // 32-bit status, encoded as `| 31:28 prefix | 27:0 data |` (big-endian)
    pub fn status_code(&self) -> u32 {
        let (prefix, data) = match &self.result {
            // prefix 0x0, followed by the HTTP status code
            Ok(response) => (0x0, (response.status as u32).to_be()),
            Err(IndexerError::NoAttestation) => (0x1, 0x0),
            Err(IndexerError::UnattestableError) => (0x2, 0x0),
            Err(IndexerError::Timeout) => (0x3, 0x0),
            Err(IndexerError::UnexpectedPayload) => (0x4, 0x0),
            Err(IndexerError::UnresolvedBlock) => (0x5, 0x0),
            // prefix 0x6, followed by a 28-bit hash of the error message
            Err(IndexerError::Other(msg)) => (0x6, sip24_hash(&msg) as u32),
        };
        (prefix << 28) | (data & (u32::MAX >> 4))
    }
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
}

pub struct InputWriters {
    pub indexer_inputs: indexer_selection::InputWriters,
    pub indexers: Arc<Indexers>,
}

impl Inputs {
    pub fn new() -> (InputWriters, Self) {
        let (indexer_input_writers, indexer_inputs) = Indexers::inputs();
        let indexers = Arc::new(Indexers::new(indexer_inputs));
        (
            InputWriters {
                indexer_inputs: indexer_input_writers,
                indexers: indexers.clone(),
            },
            Inputs { indexers },
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
        block_resolvers: Arc<HashMap<String, BlockResolver>>,
        deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
        inputs: Inputs,
    ) -> Self {
        Self {
            indexers: inputs.indexers,
            deployment_indexers,
            indexer_client,
            kafka_client,
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
        let indexers = self
            .deployment_indexers
            .value_immediate()
            .and_then(|map| map.get(&subgraph.deployment).cloned())
            .unwrap_or_default();
        tracing::info!(
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

        let mut block_resolver = self
            .block_resolvers
            .get(&subgraph.network)
            .cloned()
            .ok_or(MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let freshness_requirements =
            Indexers::freshness_requirements(&mut context, &block_resolver).await?;

        // Reject queries for blocks before minimum start block of subgraph manifest.
        match freshness_requirements.minimum_block {
            Some(min_block) if min_block < subgraph.min_block => return Err(BlockBeforeMin),
            _ => (),
        };

        let query_count = context.operations.len().max(1) as u64;
        let api_key = query.api_key.as_ref().unwrap();
        tracing::trace!(
            indexer_preferences = ?api_key.indexer_preferences,
            max_budget = ?api_key.max_budget,
        );
        // This has to run regardless even if we don't use the budget because
        // it updates the query volume estimate. This is important in the case
        // that the user switches back to automated volume discounting. Otherwise
        // it will look like there is a long period of inactivity which would increase
        // the price.
        let budget = api_key
            .usage
            .lock()
            .await
            .budget_for_queries(query_count, &self.config.budget_factors);
        let mut budget = USD::try_from(budget).unwrap();
        if let Some(max_budget) = api_key.max_budget {
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
        query.budget = Some(budget);

        let utility_config = UtilityConfig::from_preferences(&api_key.indexer_preferences);

        // Used to track instances of the latest block getting unresolved by idexers. If this
        // happens enough, we will ignore the latest block since it may be uncled.
        let mut latest_unresolved: usize = 0;

        for retry_count in 0..self.config.indexer_selection_retry_limit {
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
                    &subgraph.network,
                    &subgraph.deployment,
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
            let indexing = indexer_query.indexing.clone();
            let result = self
                .execute_indexer_query(query, indexer_query, deployment_id)
                .await;
            assert_eq!(retry_count + 1, query.indexer_attempts.len());
            let attempt = query.indexer_attempts.last_mut().unwrap();
            match result {
                Ok(()) => {
                    self.indexers
                        .observe_successful_query(&indexing, attempt.duration, &attempt.receipt)
                        .await;
                    return Ok(());
                }
                Err(err) => {
                    self.indexers
                        .observe_failed_query(&indexing, attempt.duration, &attempt.receipt, &err)
                        .await;
                    if let IndexerError::UnresolvedBlock = err {
                        self.indexers
                            .observe_indexing_behind(&mut context, &indexing, &block_resolver)
                            .await;
                        // Skip 1 block for every 2 attempts where the indexer failed to resolve
                        // the block we consider to be latest. Our latest block may be uncled.
                        latest_unresolved += 1;
                        block_resolver.skip_latest(latest_unresolved / 2);
                    }
                    attempt.result = Err(err);
                }
            };
        }
        tracing::trace!("retry limit reached");
        Err(NoIndexerSelected)
    }

    async fn execute_indexer_query(
        &self,
        query: &mut Query,
        indexer_query: IndexerQuery,
        deployment_id: &str,
    ) -> Result<(), IndexerError> {
        let indexer_id = indexer_query.indexing.indexer.to_string();
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
            indexer_errors: String::default(),
            duration: query_duration,
        });
        let result = query.indexer_attempts.last_mut().unwrap();
        let response = match &result.result {
            Ok(response) => response,
            Err(err) => {
                with_metric(
                    &METRICS.indexer_request.failed,
                    &[&deployment_id, &indexer_id],
                    |counter| counter.inc(),
                );
                return Err(err.clone());
            }
        };
        with_metric(
            &METRICS.indexer_request.ok,
            &[&deployment_id, &indexer_id],
            |counter| counter.inc(),
        );

        let indexer_errors = serde_json::from_str::<Response<Box<RawValue>>>(&response.payload)
            .map_err(|_| IndexerError::UnexpectedPayload)?
            .errors
            .unwrap_or_default()
            .into_iter()
            .map(|err| err.message)
            .collect::<Vec<String>>();
        result.indexer_errors = indexer_errors.join(",");

        if indexer_errors.iter().any(|err| {
            err == "Failed to decode `block.hash` value: `no block with that hash found`"
        }) {
            return Err(IndexerError::UnresolvedBlock);
        }

        for error in &indexer_errors {
            if UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS
                .iter()
                .any(|err| error.contains(err))
            {
                with_metric(
                    &METRICS.indexer_response_unattestable,
                    &[&deployment_id, &indexer_id],
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
            && indexer_errors
                .iter()
                .any(|err| err.contains("Null value resolved for non-null field"))
        {
            return Ok(());
        }

        let subgraph = query.subgraph.as_ref().unwrap();
        if !subgraph.features.is_empty() && response.attestation.is_none() {
            return Err(IndexerError::NoAttestation);
        }

        if let Some(attestation) = &response.attestation {
            self.challenge_indexer_response(
                indexer_query.indexing.clone(),
                result.allocation.clone(),
                result.query.clone(),
                attestation.clone(),
            );
        }

        Ok(())
    }

    fn notify_isa_sample(
        &self,
        query: &Query,
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
    }

    fn notify_isa_err(&self, query: &Query, indexer: &Address, err: SelectionError, message: &str) {
        self.kafka_client
            .send(&ISAScoringError::new(&query, &indexer, &err, message));
        // The following logs are required for data science.
        tracing::info!(
            ray_id = %query.ray_id.clone(),
            query_id = %query.id,
            deployment = %query.subgraph.as_ref().unwrap().deployment,
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
