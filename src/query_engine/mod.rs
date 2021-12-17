#[cfg(test)]
mod tests;

use crate::{
    block_resolver::BlockResolver,
    indexer_client::*,
    indexer_selection::{
        self, Context, IndexerError, IndexerQuery, Indexers, SelectionError, UnresolvedBlock,
    },
    manifest_client::{SubgraphInfo, SubgraphInfoMap},
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
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum Subgraph {
    ID(SubgraphID),
    Deployment(SubgraphDeploymentID),
}

#[derive(Clone, Debug)]
pub struct ClientQuery {
    pub id: QueryID,
    pub api_key: Arc<APIKey>,
    pub query: String,
    pub variables: Option<String>,
    pub network: String,
    pub subgraph: Subgraph,
}

#[derive(Clone, Copy)]
pub struct QueryID {
    local_id: u64,
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

#[derive(Debug)]
pub enum QueryEngineError {
    SubgraphNotFound,
    NoIndexers,
    NoIndexerSelected,
    APIKeySubgraphNotAuthorized,
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
        }
    }
}

enum RemoveIndexer {
    Yes,
    No,
}

#[derive(Clone)]
pub struct Config {
    pub network: String,
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

pub struct QueryEngine<I: IndexerInterface + Clone + Send> {
    indexers: Arc<Indexers>,
    current_deployments: Eventual<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>>,
    deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, im::Vector<Address>>>>,
    subgraph_info: SubgraphInfoMap,
    block_resolvers: Arc<HashMap<String, BlockResolver>>,
    indexer_client: I,
    config: Config,
}

impl<I: IndexerInterface + Clone + Send + 'static> QueryEngine<I> {
    pub fn new(
        config: Config,
        indexer_client: I,
        block_resolvers: Arc<HashMap<String, BlockResolver>>,
        subgraph_info: SubgraphInfoMap,
        inputs: Inputs,
    ) -> Self {
        Self {
            indexers: inputs.indexers,
            current_deployments: inputs.current_deployments,
            deployment_indexers: inputs.deployment_indexers,
            indexer_client,
            block_resolvers,
            subgraph_info,
            config,
        }
    }

    pub async fn execute_query(
        &self,
        query: ClientQuery,
    ) -> Result<QueryResponse, QueryEngineError> {
        tracing::debug!(
            query.network = %query.network,
            query.subgraph = ?query.subgraph,
            indexer_selection_retry_limit = ?self.config.indexer_selection_retry_limit
        );
        let api_key = query.api_key.key.clone();
        let query_start = Instant::now();
        let name_timer = if let Subgraph::ID(id) = &query.subgraph {
            with_metric(
                &METRICS.subgraph_name_duration,
                &[&id.to_string()],
                |hist| hist.start_timer(),
            )
        } else {
            None
        };
        let deployment = match &query.subgraph {
            Subgraph::Deployment(deployment) => deployment.clone(),
            Subgraph::ID(id) => self
                .current_deployments
                .value_immediate()
                .and_then(|map| map.get(id).cloned())
                .ok_or_else(|| QueryEngineError::SubgraphNotFound)?,
        };
        if !query.api_key.deployments.is_empty() && !query.api_key.deployments.contains(&deployment)
        {
            with_metric(
                &METRICS.queries_unauthorized_deployment,
                &[&api_key],
                |counter| counter.inc(),
            );
            return Err(QueryEngineError::APIKeySubgraphNotAuthorized);
        }
        name_timer.map(|t| t.observe_duration());
        let deployment_ipfs = deployment.ipfs_hash();
        let result = self
            .execute_deployment_query(query, deployment, &deployment_ipfs)
            .await;
        let result_counter = if let Ok(_) = result {
            &METRICS.queries_ok
        } else {
            &METRICS.queries_failed
        };
        with_metric(result_counter, &[&deployment_ipfs, &api_key], |counter| {
            counter.inc()
        });
        let query_execution_duration = Instant::now() - query_start;
        with_metric(
            &METRICS.query_duration,
            &[&deployment_ipfs, &api_key],
            |hist| {
                hist.observe(query_execution_duration.as_secs_f64());
            },
        );
        tracing::info!(query_execution_duration_ms = query_execution_duration.as_millis() as u32);
        result
    }

    #[tracing::instrument(skip(self, query, deployment_ipfs))]
    async fn execute_deployment_query(
        &self,
        query: ClientQuery,
        deployment: SubgraphDeploymentID,
        deployment_ipfs: &str,
    ) -> Result<QueryResponse, QueryEngineError> {
        use QueryEngineError::*;
        let mut indexers = self
            .deployment_indexers
            .value_immediate()
            .and_then(|map| map.get(&deployment).cloned())
            .unwrap_or_default();
        tracing::debug!(?deployment, deployment_indexers = indexers.len());
        if indexers.is_empty() {
            return Err(NoIndexers);
        }
        let _execution_timer = with_metric(
            &METRICS.query_execution_duration,
            &[&deployment_ipfs],
            |hist| hist.start_timer(),
        );

        let subgraph_info = self
            .subgraph_info
            .value_immediate()
            .and_then(|info| info.get(&deployment)?.value_immediate())
            .ok_or(SubgraphNotFound)?;

        let mut context =
            Context::new(&query.query, query.variables.as_deref().unwrap_or_default())
                .map_err(|_| MalformedQuery)?;
        let block_resolver = self
            .block_resolvers
            .get(&query.network)
            .ok_or(MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let freshness_requirements =
            Indexers::freshness_requirements(&mut context, block_resolver).await?;

        for retry_count in 0..self.config.indexer_selection_retry_limit {
            let selection_timer = with_metric(
                &METRICS.indexer_selection_duration,
                &[&deployment_ipfs],
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
                    &query.network,
                    &deployment,
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
            let indexer_query = match selection_result {
                Ok(Some(indexer_query)) => indexer_query,
                Ok(None) => return Err(NoIndexerSelected),
                Err(err) => return Err(err.into()),
            };
            let indexer = indexer_query.indexing.indexer;
            let result = self
                .execute_indexer_query(
                    &query,
                    indexer_query,
                    &deployment,
                    deployment_ipfs,
                    &subgraph_info,
                    &mut context,
                    block_resolver,
                    retry_count,
                )
                .await;
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

    async fn execute_indexer_query(
        &self,
        query: &ClientQuery,
        indexer_query: IndexerQuery,
        deployment: &SubgraphDeploymentID,
        deployment_ipfs: &str,
        subgraph_info: &SubgraphInfo,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        retry_count: usize,
    ) -> Result<QueryResponse, RemoveIndexer> {
        let indexer_id = indexer_query.indexing.indexer.to_string();
        tracing::info!(indexer = %indexer_id);
        self.observe_indexer_selection_metrics(&deployment, &indexer_query);
        let t0 = Instant::now();
        let result = self.indexer_client.query_indexer(&indexer_query).await;
        let query_duration = Instant::now() - t0;
        with_metric(
            &METRICS.indexer_requests_duration,
            &[&deployment_ipfs, &indexer_id],
            |hist| hist.observe(query_duration.as_secs_f64()),
        );

        // Stand-in for an event sent to some message bus
        let indexer_response_status = match &result {
            Ok(response) => response.status.to_string(),
            Err(err) => err.to_string(),
        };
        tracing::info!(
            api_key = %query.api_key.key,
            indexer = %indexer_query.indexing.indexer,
            indexer_url = %indexer_query.url,
            fee = %indexer_query.fee,
            blocks_behind = ?indexer_query.blocks_behind,
            indexer_query_duration_ms = query_duration.as_millis() as u32,
            %indexer_response_status,
            indexer_query = %indexer_query.query,
            %retry_count,
            "indexer query result",
        );

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                self.indexers
                    .observe_failed_query(&indexer_query.indexing, &indexer_query.receipt, err)
                    .await;
                with_metric(
                    &METRICS.indexer_requests_failed,
                    &[&deployment_ipfs, &indexer_id],
                    |counter| counter.inc(),
                );
                return Err(RemoveIndexer::Yes);
            }
        };
        with_metric(
            &METRICS.indexer_requests_ok,
            &[&deployment_ipfs, &indexer_id],
            |counter| counter.inc(),
        );

        if !subgraph_info.features.is_empty() && response.attestation.is_none() {
            tracing::info!(indexer_response_err = "Attestable response has no attestation");
            self.indexers
                .observe_failed_query(
                    &indexer_query.indexing,
                    &indexer_query.receipt,
                    IndexerError::NoAttestation,
                )
                .await;
            return Err(RemoveIndexer::Yes);
        }

        if let Err(remove_indexer) = self
            .check_unattestable_responses(context, &block_resolver, &indexer_query, &response)
            .await
        {
            with_metric(
                &METRICS.indexer_response_unattestable,
                &[&deployment_ipfs, &indexer_id],
                |counter| counter.inc(),
            );
            return Err(remove_indexer);
        }

        if let Some(_attestation) = &response.attestation {
            // TODO: fisherman
        }

        self.indexers
            .observe_successful_query(
                &indexer_query.indexing,
                query_duration,
                &indexer_query.receipt,
            )
            .await;
        Ok(QueryResponse {
            query: indexer_query,
            response,
        })
    }

    fn observe_indexer_selection_metrics(
        &self,
        deployment: &SubgraphDeploymentID,
        selection: &IndexerQuery,
    ) {
        let deployment = deployment.ipfs_hash();
        let metrics = &METRICS.indexer_selection;
        if let Ok(hist) = metrics
            .blocks_behind
            .get_metric_with_label_values(&[&deployment])
        {
            if let Some(blocks_behind) = selection.blocks_behind {
                hist.observe(blocks_behind as f64);
            }
        }
        if let Ok(hist) = metrics.fee.get_metric_with_label_values(&[&deployment]) {
            hist.observe(selection.fee.as_f64());
        }
        if let Ok(counter) = metrics
            .indexer_selected
            .get_metric_with_label_values(&[&deployment, &selection.indexing.indexer.to_string()])
        {
            counter.inc();
        }
        if let Ok(hist) = metrics
            .slashable_dollars
            .get_metric_with_label_values(&[&deployment])
        {
            hist.observe(selection.slashable_usd.as_f64());
        }
        if let Ok(hist) = metrics.utility.get_metric_with_label_values(&[&deployment]) {
            hist.observe(*selection.utility);
        }
    }

    async fn check_unattestable_responses(
        &self,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        indexer_query: &IndexerQuery,
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
                .observe_indexing_behind(context, indexer_query, block_resolver)
                .await;
            return Err(RemoveIndexer::No);
        }

        if indexer_response_has_error(&parsed_response, "panic processing query") {
            tracing::info!(indexer_response_err = "panic processing query");
            self.indexers
                .observe_failed_query(
                    &indexer_query.indexing,
                    &indexer_query.receipt,
                    IndexerError::NondeterministicResponse,
                )
                .await;
            return Err(RemoveIndexer::Yes);
        }

        Ok(())
    }
}

struct Metrics {
    indexer_response_unattestable: prometheus::IntCounterVec,
    indexer_requests_duration: prometheus::HistogramVec,
    indexer_requests_failed: prometheus::IntCounterVec,
    indexer_requests_ok: prometheus::IntCounterVec,
    indexer_selection: IndexerSelectionMetrics,
    indexer_selection_duration: prometheus::HistogramVec,
    queries_failed: prometheus::IntCounterVec,
    queries_ok: prometheus::IntCounterVec,
    queries_unauthorized_deployment: prometheus::IntCounterVec,
    query_duration: prometheus::HistogramVec,
    query_execution_duration: prometheus::HistogramVec,
    subgraph_name_duration: prometheus::HistogramVec,
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
            indexer_response_unattestable: prometheus::register_int_counter_vec!(
                "query_engine_indexer_response_unattestable",
                "Number of unattestable indexer responses",
                &["deployment", "indexer"]
            )
            .unwrap(),
            indexer_requests_duration: prometheus::register_histogram_vec!(
                "query_engine_indexer_request_duration",
                "Duration of making a request to an indexer",
                &["deployment", "indexer"]
            )
            .unwrap(),
            indexer_requests_failed: prometheus::register_int_counter_vec!(
                "query_engine_indexer_requests_failed",
                "Number of failed queries made to an indexer",
                &["deployment", "indexer"]
            )
            .unwrap(),
            indexer_requests_ok: prometheus::register_int_counter_vec!(
                "query_engine_indexer_requests_ok",
                "Number of successful requests made to an indexer",
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
            queries_failed: prometheus::register_int_counter_vec!(
                "gateway_queries_failed",
                "Queries that failed executing",
                &["deployment", "apiKey"]
            )
            .unwrap(),
            queries_ok: prometheus::register_int_counter_vec!(
                "gateway_queries_ok",
                "Successfully executed queries",
                &["deployment", "apiKey"]
            )
            .unwrap(),
            queries_unauthorized_deployment: prometheus::register_int_counter_vec!(
                "gateway_queries_for_excluded_deployment",
                "Queries for a subgraph deployment not included in an API key",
                &["apiKey"]
            )
            .unwrap(),
            query_duration: prometheus::register_histogram_vec!(
                "gateway_query_execution_duration",
                "Duration of processing a query",
                &["deployment", "apiKey"]
            )
            .unwrap(),
            query_execution_duration: prometheus::register_histogram_vec!(
                "query_engine_query_execution_duration",
                "Duration of executing the query for a deployment",
                &["deployment"]
            )
            .unwrap(),
            subgraph_name_duration: prometheus::register_histogram_vec!(
                "query_engine_subgraph_name_duration",
                "Duration of resolving a subgraph name to a deployment",
                &["name"]
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
