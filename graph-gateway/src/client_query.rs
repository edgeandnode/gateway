use crate::{
    block_constraints::{block_constraints, make_query_deterministic, BlockConstraint},
    chains::BlockCache,
    fisherman_client::{ChallengeOutcome, FishermanClient},
    graphql_error_response,
    indexer_client::{Attestation, IndexerClient, IndexerError, ResponsePayload},
    kafka_client::{
        indexer_attempt_status_code, timestamp, ClientQueryResult, IndexerAttempt, KafkaClient,
    },
    manifest_client::{SubgraphInfo, SubgraphInfoMap},
    metrics::{with_metric, METRICS},
    price_automation::QueryBudgetFactors,
    receipts::{ReceiptPools, ReceiptStatus},
    studio_client::{is_domain_authorized, APIKey, QueryStatus},
    subgraph_deployments::SubgraphDeployments,
    unattestable_errors::UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS,
};
use actix_http::StatusCode;
use actix_web::{http::header, web, HttpRequest, HttpResponse, HttpResponseBuilder};
use futures::future::join_all;
use indexer_selection::{
    actor::Update, BlockRequirements, Context as AgoraContext,
    IndexerError as IndexerSelectionError, IndexerErrorObservation, Indexing, InputError,
    Selection, UnresolvedBlock, UtilityParameters, SELECTION_LIMIT,
};
use lazy_static::lazy_static;
use prelude::{
    anyhow::{anyhow, bail, ensure},
    buffer_queue::QueueWriter,
    double_buffer::DoubleBufferReader,
    graphql::Response,
    Eventual, SubgraphDeploymentID, *,
};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use url::Url;
use uuid::Uuid;

#[derive(Copy, Clone)]
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

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub kafka_client: Arc<KafkaClient>,
    pub fisherman_client: Option<Arc<FishermanClient>>,
    pub api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    pub api_key_payment_required: bool,
    pub special_api_keys: Arc<HashSet<String>>,
    pub indexer_selection_retry_limit: usize,
    pub budget_factors: QueryBudgetFactors,
    pub block_caches: Arc<HashMap<String, BlockCache>>,
    pub subgraph_info: SubgraphInfoMap,
    pub subgraph_deployments: SubgraphDeployments,
    pub deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    pub receipt_pools: ReceiptPools,
    pub isa_state: DoubleBufferReader<indexer_selection::State>,
    pub observations: QueueWriter<Update>,
}

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    pub query: String,
    pub variables: Option<Box<RawValue>>,
}

#[derive(Debug)]
enum SubgraphResolutionError {
    InvalidSubgraphID(String),
    InvalidDeploymentID(String),
    SubgraphNotFound(String),
    DeploymentNotFound(String),
}

pub async fn handle_query(
    request: HttpRequest,
    payload: web::Json<QueryBody>,
    ctx: web::Data<Context>,
) -> HttpResponse {
    let start_time = Instant::now();
    let query_id = QueryID::new();
    let mut report = ClientQueryResult::default();
    report.query_id = query_id.to_string();
    report.ray_id = request
        .headers()
        .get("cf-ray")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    report.api_key = request
        .match_info()
        .get("api_key")
        .unwrap_or("")
        .to_string();
    let subgraph_resolution_result = resolve_subgraph_deployment(
        &ctx.subgraph_deployments,
        &ctx.subgraph_info,
        request.match_info(),
    )
    .await;
    report.deployment = subgraph_resolution_result
        .as_ref()
        .map(|i| i.deployment.to_string())
        .unwrap_or_default();
    let _span = tracing::info_span!(
        "handle_client_query",
        query_id = %report.query_id,
        ray_id = %report.ray_id,
        api_key = %report.api_key,
        deployment = %report.deployment,
    )
    .entered();
    let _timer = METRICS.client_query.start_timer(&[&report.deployment]);

    let domain = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Some(v.parse::<Url>().ok()?.host_str()?.to_string()))
        .unwrap_or("".to_string());

    let result = match subgraph_resolution_result {
        Ok(subgraph_info) => {
            handle_client_query_inner(&ctx, &mut report, subgraph_info, payload.0, domain)
                .in_current_span()
                .await
        }
        Err(err) => Err(anyhow!("{:?}", err)),
    };
    METRICS.client_query.check(&[&report.deployment], &result);

    // TODO: test reporting
    report.status = match &result {
        Ok(_) => StatusCode::OK.to_string(),
        Err(err) => err.to_string(),
    };
    report.status_code = match &result {
        Ok(_) => 0,
        Err(_) => sip24_hash(&report.status) as u32 | 0x1,
    };
    report.timestamp = timestamp();
    report.response_time_ms = (Instant::now() - start_time).as_millis() as u32;
    ctx.kafka_client.send(&report);
    // data science
    tracing::info!(
        query_id = %report.query_id,
        ray_id = %report.ray_id,
        deployment = %report.deployment,
        network = %report.network,
        api_key = %report.api_key,
        budget = %report.budget,
        fee = report.fee,
        response_time_ms = report.response_time_ms,
        status = %report.status,
        status_code = report.status_code,
        "Client query result",
    );

    let mut response_builder = HttpResponseBuilder::new(StatusCode::OK);
    response_builder.insert_header(header::ContentType::json());
    match result {
        Ok(ResponsePayload { body, attestation }) => {
            let attestation = attestation
                .as_ref()
                .and_then(|attestation| serde_json::to_string(attestation).ok())
                .unwrap_or_default();
            response_builder
                .insert_header(("Graph-Attestation", attestation))
                .body(body.as_ref())
        }
        Err(err) => graphql_error_response(err),
    }
}

async fn resolve_subgraph_deployment(
    deployments: &SubgraphDeployments,
    subgraph_info: &SubgraphInfoMap,
    params: &actix_web::dev::Path<actix_web::dev::Url>,
) -> Result<Ptr<SubgraphInfo>, SubgraphResolutionError> {
    let deployment = if let Some(id) = params.get("subgraph_id") {
        let subgraph = id
            .parse::<SubgraphID>()
            .map_err(|_| SubgraphResolutionError::InvalidSubgraphID(id.to_string()))?;
        deployments
            .current_deployment(&subgraph)
            .await
            .ok_or_else(|| SubgraphResolutionError::SubgraphNotFound(id.to_string()))?
    } else if let Some(id) = params.get("deployment_id") {
        SubgraphDeploymentID::from_ipfs_hash(id)
            .ok_or_else(|| SubgraphResolutionError::InvalidDeploymentID(id.to_string()))?
    } else {
        return Err(SubgraphResolutionError::SubgraphNotFound("".to_string()));
    };
    subgraph_info
        .value_immediate()
        .and_then(|map| map.get(&deployment)?.value_immediate())
        .map(move |info| info)
        .ok_or_else(|| SubgraphResolutionError::DeploymentNotFound(deployment.to_string()))
}

async fn handle_client_query_inner(
    ctx: &Context,
    report: &mut ClientQueryResult,
    subgraph_info: Ptr<SubgraphInfo>,
    payload: QueryBody,
    domain: String,
) -> anyhow::Result<ResponsePayload> {
    let api_key = ctx
        .api_keys
        .value_immediate()
        .unwrap_or_default()
        .get(&report.api_key)
        .cloned()
        .ok_or_else(|| anyhow!("Invalid API key"))?;

    if ctx.api_key_payment_required
        && !api_key.is_subsidized
        && !ctx.special_api_keys.contains(&api_key.key)
    {
        // Enforce the API key payment status, unless it's being subsidized.
        match api_key.query_status {
            QueryStatus::Active => (),
            QueryStatus::Inactive => bail!("Querying not activated yet; make sure to add some GRT to your balance in the studio"),
            QueryStatus::ServiceShutoff => bail!("Payment required for subsequent requests for this API key"),
        };
    }
    tracing::debug!(%domain, authorized = ?api_key.domains);
    if !api_key.domains.is_empty() && !is_domain_authorized(&api_key.domains, &domain) {
        bail!("Domain not authorized by API key");
    }
    let deployment_authorized =
        api_key.deployments.is_empty() || api_key.deployments.contains(&subgraph_info.deployment);
    let subgraph_authorized =
        api_key.subgraphs.is_empty() || api_key.subgraphs.contains(&subgraph_info.id);
    if !deployment_authorized || !subgraph_authorized {
        bail!("Subgraph not authorized by API key");
    }

    let deployment_indexers = ctx
        .deployment_indexers
        .value_immediate()
        .and_then(|map| map.get(&subgraph_info.deployment).cloned())
        .unwrap_or_default();
    tracing::info!(deployment_indexers = deployment_indexers.len());
    ensure!(
        !deployment_indexers.is_empty(),
        "No indexers found for subgraph deployment"
    );

    let variables = payload
        .variables
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let context = AgoraContext::new(&payload.query, &variables)?;
    // data science
    tracing::info!(
        query_id = %report.query_id,
        ray_id = %report.ray_id,
        query = %payload.query,
        %variables,
        "Client query",
    );

    let mut block_cache = ctx
        .block_caches
        .get(&subgraph_info.network)
        .cloned()
        .ok_or_else(|| anyhow!("Network not supported: {}", &subgraph_info.network))?;

    let block_constraints = block_constraints(&context).ok_or(anyhow!("Invalid query"))?;
    let resolved_blocks = join_all(
        block_constraints
            .iter()
            .filter_map(|constraint| constraint.clone().into_unresolved())
            .map(|unresolved| block_cache.fetch_block(unresolved)),
    )
    .await
    .into_iter()
    .collect::<Result<BTreeSet<BlockPointer>, UnresolvedBlock>>()
    .map_err(|unresolved| anyhow!("Unresolved block: {}", unresolved))?;
    let min_block = resolved_blocks.iter().map(|b| b.number).min();
    let max_block = resolved_blocks.iter().map(|b| b.number).max();
    let block_requirements = BlockRequirements {
        range: min_block.map(|min| (min, max_block.unwrap())),
        has_latest: block_constraints.iter().any(|c| match c {
            BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => true,
            BlockConstraint::Hash(_) | BlockConstraint::Number(_) => false,
        }),
    };

    // Reject queries for blocks before minimum start block of subgraph manifest.
    ensure!(
        !matches!(min_block, Some(min_block) if min_block < subgraph_info.min_block),
        "Requested block before minimum `startBlock` of subgraph manifest: {}",
        min_block.unwrap_or_default()
    );

    let top_level_operations = context.operations.len().max(1) as u64;
    tracing::trace!(
        indexer_preferences = ?api_key.indexer_preferences,
        max_budget = ?api_key.max_budget,
    );
    // This has to run even if we don't use the budget because it updates the query volume estimate.
    // This is important in the case that the user switches back to automated volume discounting.
    // Otherwise it will look like there is a long period of inactivity which would increase the
    // price.
    let budget = api_key
        .usage
        .lock()
        .await
        .budget_for_queries(top_level_operations, &ctx.budget_factors);
    let mut budget = USD::try_from(budget).unwrap();
    if let Some(max_budget) = api_key.max_budget {
        // Security: Consumers can and will set their budget to unreasonably high values. This
        // `.min` prevents the budget from being set far beyond what it would be automatically. The
        // reason this is important is because sometimes queries are subsidized and we would be
        // at-risk to allow arbitrarily high values.
        budget = max_budget.min(budget * USD::try_from(10_u64).unwrap());
    }
    let budget: GRT = ctx
        .isa_state
        .latest()
        .network_params
        .usd_to_grt(USD::try_from(budget).unwrap())
        .ok_or_else(|| anyhow!("Internal error: MissingExchangeRate"))?;
    report.budget = budget.to_string();

    let mut utility_params = UtilityParameters::new(
        budget,
        block_requirements,
        0, // 170cbcf3-db7f-404a-be13-2022d9142677
        api_key.indexer_preferences.performance,
        api_key.indexer_preferences.data_freshness,
        api_key.indexer_preferences.economic_security,
        api_key.indexer_preferences.price_efficiency,
    );

    let mut total_indexer_queries = 0;
    // Used to track how many times an indexer failed to resolve a block. This may indicate that
    // our latest block has been uncled.
    let mut latest_unresolved: u64 = 0;

    for retry in 0..ctx.indexer_selection_retry_limit {
        let last_retry = retry == (ctx.indexer_selection_retry_limit - 1);
        // Make sure our observations are up-to-date if retrying.
        if retry > 0 {
            let _ = ctx.observations.flush().await;
        }

        let latest_block = block_cache
            .chain_head
            .value_immediate()
            .ok_or_else(|| anyhow!("Unresolved block: 0"))?;
        tracing::debug!(?latest_block);
        // 170cbcf3-db7f-404a-be13-2022d9142677
        utility_params.latest_block = latest_block.number;

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
            &[&report.deployment],
            |hist| hist.start_timer(),
        );
        let (selections, indexer_errors) = ctx
            .isa_state
            .latest()
            .select_indexers(
                &subgraph_info.deployment,
                &deployment_indexers,
                &utility_params,
                &mut context,
                SELECTION_LIMIT as u8,
            )
            .map_err(|err| match err {
                InputError::MalformedQuery => anyhow!("Invalid query"),
                InputError::MissingNetworkParams => anyhow!("Internal error: MissingNetworkParams"),
            })?;
        drop(selection_timer);

        let selections_len = selections.len();
        total_indexer_queries += selections_len;

        tracing::debug!(indexer_errors = ?indexer_errors.0);
        if selections.is_empty() {
            let isa_errors = indexer_errors
                .0
                .iter()
                .map(|(k, v)| (k, v.len()))
                .filter(|(_, l)| *l > 0)
                .collect::<BTreeMap<&IndexerSelectionError, usize>>();
            bail!(
                "No suitable indexer found for subgraph deployment. Indexer selection errors: {:?}",
                isa_errors
            );
        }

        let mut indexer_query_context = IndexerQueryContext {
            indexer_client: ctx.indexer_client.clone(),
            kafka_client: ctx.kafka_client.clone(),
            fisherman_client: ctx.fisherman_client.clone(),
            receipt_pools: ctx.receipt_pools.clone(),
            observations: ctx.observations.clone(),
            subgraph_info: subgraph_info.clone(),
            latest_block: latest_block.number,
            report: IndexerAttempt::default(),
        };
        indexer_query_context.report.query_id = report.query_id.clone();
        indexer_query_context.report.ray_id = report.ray_id.clone();
        indexer_query_context.report.api_key = report.api_key.clone();
        indexer_query_context.report.deployment = report.deployment.clone();

        let (response_tx, mut response_rx) = mpsc::channel(SELECTION_LIMIT);
        for selection in selections {
            let latest_query_block = match block_cache
                .latest(selection.blocks_behind + latest_unresolved)
                .await
            {
                Ok(latest_query_block) => latest_query_block,
                Err(_) if !last_retry => continue,
                Err(unresolved) => bail!("Unresolved block: {}", unresolved),
            };
            let deterministic_query =
                make_query_deterministic(context.clone(), &resolved_blocks, &latest_query_block)
                    .ok_or_else(|| anyhow!("Invalid query"))?;

            let indexer_query_context = indexer_query_context.clone();
            let response_tx = response_tx.clone();
            tokio::spawn(async move {
                let response = handle_indexer_query(
                    indexer_query_context,
                    selection,
                    deterministic_query,
                    latest_query_block.number,
                )
                .await;
                let _ = response_tx.try_send(response);
            });
        }
        for _ in 0..selections_len {
            match response_rx.recv().await {
                Some(Ok(payload)) => return Ok(payload),
                Some(Err(IndexerError::UnresolvedBlock)) => latest_unresolved += 1,
                Some(Err(_)) | None => (),
            };
        }
    }

    bail!(
        "No suitable indexer found for subgraph deployment. Indexer queries attempted: {}",
        total_indexer_queries
    );
}

#[derive(Clone)]
struct IndexerQueryContext {
    pub indexer_client: IndexerClient,
    pub kafka_client: Arc<KafkaClient>,
    pub fisherman_client: Option<Arc<FishermanClient>>,
    pub receipt_pools: ReceiptPools,
    pub observations: QueueWriter<Update>,
    pub subgraph_info: Ptr<SubgraphInfo>,
    pub latest_block: u64,
    pub report: IndexerAttempt,
}

async fn handle_indexer_query(
    mut ctx: IndexerQueryContext,
    selection: Selection,
    deterministic_query: String,
    latest_query_block: u64,
) -> Result<ResponsePayload, IndexerError> {
    let indexing = selection.indexing.clone();
    ctx.report.indexer = indexing.indexer.to_string();
    ctx.report.url = selection.url.to_string();
    ctx.report.fee = selection.fee.as_f64();
    ctx.report.utility = 1.0; // for backwards compatibility
    ctx.report.blocks_behind = selection.blocks_behind;

    let receipt = ctx
        .receipt_pools
        .commit(&selection.indexing, selection.fee)
        .await
        .map_err(|_| IndexerError::NoAllocation);

    let result = match receipt.as_ref() {
        Ok(receipt) => {
            handle_indexer_query_inner(&mut ctx, selection, deterministic_query, &receipt).await
        }
        Err(err) => Err(err.clone()),
    };
    METRICS
        .indexer_query
        .check(&[&ctx.report.deployment], &result);

    ctx.report.status = match &result {
        Ok(_) => StatusCode::OK.to_string(),
        Err(err) => format!("{:?}", err),
    };
    ctx.report.status_code = indexer_attempt_status_code(&result);

    let observation = match &result {
        Ok(_) => Ok(()),
        Err(IndexerError::Timeout) => Err(IndexerErrorObservation::Timeout),
        Err(IndexerError::UnresolvedBlock) => Err(IndexerErrorObservation::IndexingBehind {
            latest_query_block,
            latest_block: ctx.latest_block,
        }),
        Err(_) => Err(IndexerErrorObservation::Other),
    };
    if let Ok(receipt) = receipt {
        let receipt_status = match &observation {
            Ok(()) => ReceiptStatus::Success,
            // The indexer is potentially unaware that it failed, since it may have sent a response back
            // with an attestation.
            Err(IndexerErrorObservation::Timeout) => ReceiptStatus::Unknown,
            Err(_) => ReceiptStatus::Failure,
        };
        ctx.receipt_pools
            .release(&indexing, &receipt, receipt_status)
            .await;
    }

    let _ = ctx.observations.write(Update::QueryObservation {
        indexing,
        duration: Duration::from_millis(ctx.report.response_time_ms as u64),
        result: observation,
    });

    ctx.report.timestamp = timestamp();
    ctx.kafka_client.send(&ctx.report);
    // data science
    tracing::info!(
        query_id = %ctx.report.query_id,
        ray_id = %ctx.report.ray_id,
        api_key = %ctx.report.api_key,
        deployment = %ctx.report.deployment,
        attempt_index = 0, // for backwards compatibility
        indexer = %ctx.report.indexer,
        url = %ctx.report.url,
        allocation = %ctx.report.allocation,
        fee = ctx.report.fee,
        blocks_behind = ctx.report.blocks_behind,
        indexer_errors = %ctx.report.indexer_errors,
        response_time_ms = %ctx.report.response_time_ms,
        status = %ctx.report.status,
        status_code = %ctx.report.status_code,
        "Indexer attempt",
    );

    result
}

async fn handle_indexer_query_inner(
    ctx: &mut IndexerQueryContext,
    selection: Selection,
    deterministic_query: String,
    receipt: &[u8],
) -> Result<ResponsePayload, IndexerError> {
    let start_time = Instant::now();
    let result = ctx
        .indexer_client
        .query_indexer(&selection, deterministic_query.clone(), receipt)
        .await;
    ctx.report.response_time_ms = (Instant::now() - start_time).as_millis() as u32;
    with_metric(
        &METRICS.indexer_query.duration,
        &[&ctx.report.deployment],
        |hist| hist.observe(ctx.report.response_time_ms as f64),
    );

    let mut allocation = Address([0; 20]);
    allocation.0.copy_from_slice(&receipt[0..20]);
    ctx.report.allocation = allocation.to_string();

    let response = result?;
    if response.status != StatusCode::OK.as_u16() {
        tracing::warn!(indexer_response_status = %response.status);
    }

    let indexer_errors = serde_json::from_str::<Response<Box<RawValue>>>(&response.payload.body)
        .map_err(|_| IndexerError::UnexpectedPayload)?
        .errors
        .unwrap_or_default()
        .into_iter()
        .map(|err| err.message)
        .collect::<Vec<String>>();
    ctx.report.indexer_errors = indexer_errors.join(",");
    if !indexer_errors.is_empty() {
        tracing::debug!(indexer_errors = %ctx.report.indexer_errors);
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
            let _ = ctx.observations.write(Update::Penalty {
                indexing: selection.indexing,
                weight: 35,
            });
            tracing::info!(%error, "penalizing for unattestable error");
            return Err(IndexerError::UnattestableError);
        }
    }

    // TODO: This is a temporary hack to handle NonNullError being incorrectly categorized as
    // unattestable in graph-node.
    if response.payload.attestation.is_none()
        && indexer_errors
            .iter()
            .any(|err| err.contains("Null value resolved for non-null field"))
    {
        return Ok(response.payload);
    }

    if !ctx.subgraph_info.features.is_empty() && response.payload.attestation.is_none() {
        return Err(IndexerError::NoAttestation);
    }

    if let Some(attestation) = &response.payload.attestation {
        challenge_indexer_response(
            ctx.fisherman_client.clone(),
            ctx.observations.clone(),
            selection.indexing.clone(),
            allocation.clone(),
            deterministic_query,
            response.payload.body.clone(),
            attestation.clone(),
        );
    }

    Ok(response.payload)
}

fn challenge_indexer_response(
    fisherman_client: Option<Arc<FishermanClient>>,
    observations: QueueWriter<Update>,
    indexing: Indexing,
    allocation: Address,
    indexer_query: String,
    indexer_response: Arc<String>,
    attestation: Attestation,
) {
    let fisherman = match fisherman_client {
        Some(fisherman) => fisherman,
        None => return,
    };
    tokio::spawn(async move {
        let outcome = fisherman
            .challenge(
                &indexing,
                &allocation,
                &indexer_query,
                &indexer_response,
                &attestation,
            )
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
