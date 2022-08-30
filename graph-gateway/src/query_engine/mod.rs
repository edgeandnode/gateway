mod clock;
mod price_automation;
#[cfg(test)]
mod tests;
mod unattestable_errors;

use crate::{
    block_resolver::*,
    fisherman_client::*,
    indexer_client::*,
    kafka_client::{ISAScoringError, ISAScoringSample, KafkaInterface},
    manifest_client::SubgraphInfo,
    metrics::*,
};
use indexer_selection::{
    self,
    actor::IndexerErrorObservation,
    receipts::{BorrowFail, QueryStatus as ReceiptStatus, ReceiptPool},
    Context, IndexerError, IndexerPreferences, IndexerQuery, IndexerScore, SelectionError,
    UnresolvedBlock,
};
pub use indexer_selection::{actor::Update, Indexing, UtilityConfig};
use lazy_static::lazy_static;
use prelude::{buffer_queue::QueueWriter, double_buffer::DoubleBufferReader, graphql::Response, *};
pub use price_automation::{QueryBudgetFactors, VolumeEstimator};
use primitive_types::U256;
use secp256k1::SecretKey;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{
    collections::{hash_map::Entry, HashMap},
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use tokio::sync::{Mutex, RwLock};
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
    pub receipt: Vec<u8>,
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
            Err(IndexerError::MissingAllocation) => (0x7, 0x0),
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
    pub query_status: QueryStatus,
    pub max_budget: Option<USD>,
    pub deployments: Vec<SubgraphDeploymentID>,
    pub subgraphs: Vec<(SubgraphID, i32)>,
    pub domains: Vec<(String, i32)>,
    pub indexer_preferences: IndexerPreferences,
    pub usage: Arc<Mutex<VolumeEstimator>>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryStatus {
    #[default]
    Inactive,
    Active,
    ServiceShutoff,
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
    MissingNetworkParams,
    MissingExchangeRate,
}

impl From<SelectionError> for QueryEngineError {
    fn from(from: SelectionError) -> Self {
        match from {
            SelectionError::BadIndexer(_) | SelectionError::NoAllocation(_) => {
                Self::NoIndexerSelected
            }
            SelectionError::BadInput => Self::MalformedQuery,
            SelectionError::MissingBlock(unresolved) => Self::MissingBlock(unresolved),
            SelectionError::FeesTooHigh(count) => Self::FeesTooHigh(count),
            SelectionError::MissingNetworkParams => Self::MissingNetworkParams,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub indexer_selection_retry_limit: usize,
    pub budget_factors: QueryBudgetFactors,
}

#[derive(Clone, Default)]
pub struct ReceiptPools {
    pools: Arc<RwLock<HashMap<Indexing, Arc<Mutex<ReceiptPool>>>>>,
}

impl ReceiptPools {
    async fn get(&self, indexing: &Indexing) -> Arc<Mutex<ReceiptPool>> {
        if let Some(pool) = self.pools.read().await.get(indexing) {
            return pool.clone();
        }
        let mut pools = self.pools.write().await;
        match pools.entry(indexing.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let pool = Arc::new(Mutex::default());
                entry.insert(pool.clone());
                pool
            }
        }
    }

    pub async fn commit(&self, indexing: &Indexing, fee: GRT) -> Result<Vec<u8>, BorrowFail> {
        let pool = self
            .pools
            .read()
            .await
            .get(indexing)
            .cloned()
            .ok_or(BorrowFail::NoAllocation)?;
        let mut pool = pool.lock().await;
        pool.commit(fee.as_u256())
    }

    pub async fn release(&self, indexing: &Indexing, receipt: &[u8], status: ReceiptStatus) {
        if receipt.len() != 164 {
            panic!("Unrecognized receipt format");
        }
        let pool = self.pools.read().await;
        let mut pool = match pool.get(indexing) {
            Some(pool) => pool.lock().await,
            None => return,
        };
        pool.release(receipt, status);
    }

    pub async fn update_receipt_pool(
        &self,
        signer: &SecretKey,
        indexing: &Indexing,
        new_allocations: &HashMap<Address, GRT>,
    ) {
        let pool = self.get(indexing).await;
        let mut pool = pool.lock().await;
        // Remove allocations not present in new_allocations
        for old_allocation in pool.addresses() {
            if new_allocations
                .iter()
                .all(|(id, _)| &old_allocation != id.as_ref())
            {
                pool.remove_allocation(&old_allocation);
            }
        }
        // Add new_allocations not present in allocations
        for (id, _) in new_allocations {
            if !pool.contains_allocation(&id) {
                pool.add_allocation(signer.clone(), id.0);
            }
        }
    }
}

pub struct QueryEngine<I, K, F>
where
    I: IndexerInterface + Clone + Send,
    K: KafkaInterface + Send,
    F: FishermanInterface + Clone + Send,
{
    pub config: Config,
    pub indexer_client: I,
    pub kafka_client: Arc<K>,
    pub fisherman_client: Option<Arc<F>>,
    pub deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    pub block_resolvers: Arc<HashMap<String, BlockResolver>>,
    pub receipt_pools: ReceiptPools,
    pub isa: DoubleBufferReader<indexer_selection::State>,
    pub observations: QueueWriter<Update>,
}

impl<I, K, F> QueryEngine<I, K, F>
where
    I: IndexerInterface + Clone + Send + 'static,
    K: KafkaInterface + Send,
    F: FishermanInterface + Clone + Send + Sync + 'static,
{
    pub async fn execute_query(&self, query: &mut Query) -> Result<(), QueryEngineError> {
        let deployment_id = query.subgraph.as_ref().unwrap().deployment.ipfs_hash();
        let _timer = METRICS.client_query.start_timer(&[&deployment_id]);
        let result = self.execute_deployment_query(query, &deployment_id).await;
        METRICS.client_query.check(&[&deployment_id], &result);
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
        // TODO: Why is this here?
        let deployment_indexers = self
            .deployment_indexers
            .value_immediate()
            .and_then(|map| map.get(&subgraph.deployment).cloned())
            .unwrap_or_default();
        tracing::info!(
            deployment = ?subgraph.deployment, deployment_indexers = deployment_indexers.len(),
        );
        if deployment_indexers.is_empty() {
            return Err(NoIndexers);
        }

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
            indexer_selection::freshness_requirements(&mut context.operations, &block_resolver)
                .await?;

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
        let isa = self.isa.latest();
        let budget: GRT = isa
            .network_params
            .usd_to_grt(USD::try_from(budget).unwrap())
            .ok_or(QueryEngineError::MissingExchangeRate)?;
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

            let selection_result = isa
                .select_indexer(
                    &utility_config,
                    &subgraph.network,
                    &subgraph.deployment,
                    &deployment_indexers,
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

            let indexing = indexer_query.indexing;
            let receipt = self
                .receipt_pools
                .commit(&indexing, indexer_query.score.fee)
                .await;
            let result = match receipt {
                Ok(receipt) => {
                    self.execute_indexer_query(query, indexer_query, receipt, deployment_id)
                        .await
                }
                Err(BorrowFail::NoAllocation) => Err(IndexerError::MissingAllocation),
            };
            assert_eq!(retry_count + 1, query.indexer_attempts.len());
            let attempt = query.indexer_attempts.last_mut().unwrap();

            let observation = match &result {
                Ok(()) => Ok(()),
                Err(IndexerError::Timeout) => Err(IndexerErrorObservation::Timeout),
                Err(IndexerError::UnresolvedBlock) => {
                    // Get this early to be closer to the time when the query was made so that race
                    // conditions occur less frequently. They will still occur though.
                    let latest = block_resolver.latest_block().map(|b| b.number).unwrap_or(0);
                    match indexer_selection::freshness_requirements(
                        &mut context.operations,
                        &block_resolver,
                    )
                    .await
                    {
                        Ok(refreshed_requirements) => {
                            Err(IndexerErrorObservation::IndexingBehind {
                                refreshed_requirements,
                                latest,
                            })
                        }
                        // If we observed a block hash in the query that we could no longer
                        // associate with a number, then we have detected a reorg and the indexer
                        // receives no penalty.
                        Err(_) => Err(IndexerErrorObservation::Other),
                    }
                }
                Err(_) => Err(IndexerErrorObservation::Other),
            };

            let receipt_status = match &observation {
                Ok(()) => ReceiptStatus::Success,
                // The indexer is potentially unaware that it failed, since it may have sent a
                // response back with an attestation.
                Err(IndexerErrorObservation::Timeout) => ReceiptStatus::Unknown,
                Err(_) => ReceiptStatus::Failure,
            };
            self.receipt_pools
                .release(&indexing, &attempt.receipt, receipt_status)
                .await;

            let _ = self.observations.write(Update::QueryObservation {
                indexing,
                duration: attempt.duration,
                result: observation,
            });

            match result {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => {
                    if let IndexerError::UnresolvedBlock = err {
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
        receipt: Vec<u8>,
        deployment_id: &str,
    ) -> Result<(), IndexerError> {
        let t0 = Instant::now();
        let result = self
            .indexer_client
            .query_indexer(&indexer_query, &receipt)
            .await;
        let query_duration = Instant::now() - t0;
        with_metric(&METRICS.indexer_query.duration, &[&deployment_id], |hist| {
            hist.observe(query_duration.as_secs_f64())
        });

        let mut allocation = Address([0; 20]);
        allocation.0.copy_from_slice(&receipt[0..20]);

        query.indexer_attempts.push(IndexerAttempt {
            score: indexer_query.score,
            indexer: indexer_query.indexing.indexer,
            allocation,
            query: Arc::new(indexer_query.query),
            receipt,
            result,
            indexer_errors: String::default(),
            duration: query_duration,
        });
        let result = query.indexer_attempts.last_mut().unwrap();
        METRICS
            .indexer_query
            .check(&[deployment_id], &result.result);
        let response = result.result.as_ref().map_err(|err| err.clone())?;

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
                let _ = self.observations.write(Update::Penalty {
                    indexing: indexer_query.indexing,
                    weight: 35,
                });
                tracing::info!("penalizing for unattestable error");
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
        let observations = self.observations.clone();
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
                let _ = observations.write(Update::Penalty {
                    indexing,
                    weight: penalty,
                });
            }
        });
    }
}
