use crate::{
    block_constraints::{block_constraints, make_query_deterministic, BlockConstraint},
    chains::*,
    fisherman_client::*,
    indexer_client::*,
    kafka_client::{ISAScoringError, KafkaInterface},
    manifest_client::SubgraphInfo,
    metrics::*,
    price_automation::*,
    receipts::*,
    unattestable_errors::UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS,
};
use futures::future::join_all;
use indexer_selection::{
    self, Context, FreshnessRequirements, IndexerError as IndexerSelectionError,
    IndexerErrorObservation, InputError, Selection, UnresolvedBlock, UtilityParameters,
};
use indexer_selection::{actor::Update, Indexing};
use lazy_static::lazy_static;
use prelude::{buffer_queue::QueueWriter, double_buffer::DoubleBufferReader, graphql::Response, *};
use primitive_types::U256;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug)]
pub struct Query {
    pub id: QueryID,
    pub ray_id: String,
    pub start_time: Instant,
    pub query: Arc<String>,
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
            query: Arc::new(query),
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
    pub selection: Selection,
    pub allocation: Address,
    pub query: Arc<String>,
    pub receipt: Receipt,
    pub result: Result<IndexerResponse, IndexerError>,
    pub indexer_errors: String,
    pub duration: Duration,
}

#[derive(Clone)]
pub struct Receipt(pub Vec<u8>);

impl fmt::Debug for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(&self.0))
    }
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
            Err(IndexerError::NoAllocation) => (0x7, 0x0),
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

#[derive(Clone, Debug, Default)]
pub struct IndexerPreferences {
    pub freshness_requirements: f64,
    pub performance: f64,
    pub data_freshness: f64,
    pub economic_security: f64,
    pub price_efficiency: f64,
}

#[derive(Debug)]
pub struct QueryResponse {
    pub selection: Selection,
    pub query: String,
    pub response: IndexerResponse,
}

#[derive(Debug, PartialEq)]
pub enum QueryEngineError {
    NoIndexers,
    NoIndexerSelected,
    IndexerSelectionErrors(String),
    MalformedQuery,
    BlockBeforeMin,
    NetworkNotSupported(String),
    MissingBlock(UnresolvedBlock),
    MissingNetworkParams,
    MissingExchangeRate,
    ExcessiveFee,
}

impl From<InputError> for QueryEngineError {
    fn from(from: InputError) -> Self {
        match from {
            InputError::MalformedQuery => Self::MalformedQuery,
            InputError::MissingNetworkParams => Self::MissingNetworkParams,
        }
    }
}

impl From<UnresolvedBlock> for QueryEngineError {
    fn from(from: UnresolvedBlock) -> Self {
        Self::MissingBlock(from)
    }
}

#[derive(Clone)]
pub struct Config {
    pub indexer_selection_retry_limit: usize,
    pub budget_factors: QueryBudgetFactors,
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
    pub block_caches: Arc<HashMap<String, BlockCache>>,
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
        let context = Context::new(&query_body, query_variables.as_deref().unwrap_or_default())
            .map_err(|_| MalformedQuery)?;

        let mut block_cache = self
            .block_caches
            .get(&subgraph.network)
            .cloned()
            .ok_or_else(|| NetworkNotSupported(subgraph.network.clone()))?;

        let block_constraints = block_constraints(&context).ok_or(MalformedQuery)?;
        let block_requirements = join_all(
            block_constraints
                .iter()
                .filter_map(|constraint| constraint.clone().into_unresolved())
                .map(|unresolved| block_cache.fetch_block(unresolved)),
        )
        .await
        .into_iter()
        .collect::<Result<BTreeSet<BlockPointer>, UnresolvedBlock>>()?;

        let freshness_requirements = FreshnessRequirements {
            minimum_block: block_requirements.iter().map(|b| b.number).max(),
            has_latest: block_constraints.iter().any(|c| match c {
                BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => true,
                BlockConstraint::Hash(_) | BlockConstraint::Number(_) => false,
            }),
        };

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
        // This has to run even if we don't use the budget because it updates the query volume
        // estimate. This is important in the case that the user switches back to automated volume
        // discounting. Otherwise it will look like there is a long period of inactivity which would
        // increase the price.
        let budget = api_key
            .usage
            .lock()
            .await
            .budget_for_queries(query_count, &self.config.budget_factors);
        let mut budget = USD::try_from(budget).unwrap();
        if let Some(max_budget) = api_key.max_budget {
            // Security: Consumers can and will set their budget to unreasonably high values. This
            // .min prevents the budget from being set beyond what it would be automatically. The
            // reason this is important is because sometimes queries are subsidized and we would be
            // at-risk to allow arbitrarily high values.
            budget = max_budget.min(budget * U256::from(10u8));
        }
        let budget: GRT = self
            .isa
            .latest()
            .network_params
            .usd_to_grt(USD::try_from(budget).unwrap())
            .ok_or(MissingExchangeRate)?;
        query.budget = Some(budget);

        let utility_params = UtilityParameters::new(
            budget,
            freshness_requirements,
            api_key.indexer_preferences.performance,
            api_key.indexer_preferences.data_freshness,
            api_key.indexer_preferences.economic_security,
            api_key.indexer_preferences.price_efficiency,
        );

        // Used to track how many times an indexer failed to resolve a block. This may indicate that
        // our latest block has been uncled.
        let mut latest_unresolved: u64 = 0;

        for retry_count in 0..self.config.indexer_selection_retry_limit {
            let latest_block = block_cache
                .chain_head
                .value_immediate()
                .ok_or(UnresolvedBlock::WithNumber(0))?;
            tracing::debug!(?latest_block);

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

            let selection_timer = with_metric(
                &METRICS.indexer_selection_duration,
                &[&deployment_id],
                |hist| hist.start_timer(),
            );
            let (selections, indexer_errors) = self.isa.latest().select_indexers(
                &subgraph.deployment,
                &deployment_indexers,
                &utility_params,
                &mut context,
                1,
            )?;
            drop(selection_timer);

            tracing::debug!(indexer_errors = ?indexer_errors.0);
            for (err, indexers) in &indexer_errors.0 {
                for indexer in indexers {
                    self.notify_isa_err(&query, indexer, err, "ISA scoring sample");
                }
            }

            let selection = match selections.into_iter().next() {
                Some(selection) => selection,
                None if indexer_errors.0.is_empty() => return Err(NoIndexerSelected),
                None => {
                    let errors = indexer_errors
                        .0
                        .iter()
                        .map(|(k, v)| (k, v.len()))
                        .filter(|(_, l)| *l > 0)
                        .collect::<BTreeMap<&IndexerSelectionError, usize>>();
                    return Err(IndexerSelectionErrors(format!("{:?}", errors)));
                }
            };
            if selection.price > GRT::try_from(100u64).unwrap() {
                tracing::error!(excessive_fee = %selection.price);
                return Err(QueryEngineError::ExcessiveFee);
            }

            // Select the latest block for the selected indexer. Also, skip 1 block for every 2
            // attempts where the indexer failed to resolve the block we consider to be latest.
            let blocks_behind = selection.blocks_behind + (latest_unresolved / 2);
            let latest_query_block = block_cache.latest(blocks_behind).await?;
            let deterministic_query =
                make_query_deterministic(context, &block_requirements, &latest_query_block)
                    .ok_or(MalformedQuery)?;

            let indexing = selection.indexing;
            let result = self
                .execute_indexer_query(query, selection, deterministic_query, deployment_id)
                .await;

            METRICS.indexer_query.check(&[deployment_id], &result);
            assert_eq!(retry_count + 1, query.indexer_attempts.len());
            let attempt = query.indexer_attempts.last_mut().unwrap();

            let observation = match &result {
                Ok(()) => Ok(()),
                Err(IndexerError::Timeout) => Err(IndexerErrorObservation::Timeout),
                Err(IndexerError::UnresolvedBlock) => {
                    // Get this early to be closer to the time when the query was made so that race
                    // conditions occur less frequently. They will still occur though.
                    let latest_block = block_cache
                        .chain_head
                        .value_immediate()
                        .map(|b| b.number)
                        .unwrap_or(0);
                    Err(IndexerErrorObservation::IndexingBehind {
                        latest_query_block: latest_query_block.number,
                        latest_block,
                    })
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
                .release(&indexing, &attempt.receipt.0, receipt_status)
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
                        latest_unresolved += 1;
                    }
                    attempt.result = Err(err);
                    // If, for example, we need to back out an optimistic prediction of an indexer's freshness,
                    // we should wait for that update to be available to the ISA before making the
                    // selection again. Otherwise the indexer will be sent the same failed block.
                    let _ = self.observations.flush().await;
                }
            };
        }
        tracing::trace!("retry limit reached");
        Err(NoIndexerSelected)
    }

    async fn execute_indexer_query(
        &self,
        query: &mut Query,
        selection: Selection,
        deterministic_query: String,
        deployment_id: &str,
    ) -> Result<(), IndexerError> {
        let receipt = self
            .receipt_pools
            .commit(&selection.indexing, selection.price)
            .await
            .map(Receipt)
            .map_err(|_| IndexerError::NoAllocation)?;

        let t0 = Instant::now();
        let result = self
            .indexer_client
            .query_indexer(&selection, deterministic_query.clone(), &receipt.0)
            .await;
        let query_duration = Instant::now() - t0;
        with_metric(&METRICS.indexer_query.duration, &[&deployment_id], |hist| {
            hist.observe(query_duration.as_secs_f64())
        });

        let mut allocation = Address([0; 20]);
        allocation.0.copy_from_slice(&receipt.0[0..20]);

        query.indexer_attempts.push(IndexerAttempt {
            selection: selection.clone(),
            allocation,
            query: Arc::new(deterministic_query),
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

        if !indexer_errors.is_empty() {
            tracing::debug!(
                errors = %result.indexer_errors,
                query = %result.query,
                "indexer errors",
            );
        }

        if indexer_errors.iter().any(|err| {
            err.contains("Failed to decode `block.hash` value")
                || err.contains("Failed to decode `block.number` value")
        }) {
            return Err(IndexerError::UnresolvedBlock);
        }

        for error in &indexer_errors {
            if UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS
                .iter()
                .any(|err| error.contains(err))
            {
                let _ = self.observations.write(Update::Penalty {
                    indexing: selection.indexing,
                    weight: 35,
                });
                tracing::info!(%error, "penalizing for unattestable error");
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
                selection.indexing.clone(),
                result.allocation.clone(),
                result.query.clone(),
                attestation.clone(),
            );
        }

        Ok(())
    }

    fn notify_isa_err(
        &self,
        query: &Query,
        indexer: &Address,
        err: &IndexerSelectionError,
        message: &str,
    ) {
        self.kafka_client
            .send(&ISAScoringError::new(&query, &indexer, err, message));
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
