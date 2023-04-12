use crate::{
    auth::{AuthHandler, AuthToken},
    block_constraints::{block_constraints, make_query_deterministic, BlockConstraint},
    chains::BlockCache,
    fisherman_client::{ChallengeOutcome, FishermanClient},
    graphql_error_response,
    indexer_client::{Attestation, IndexerClient, IndexerError, ResponsePayload},
    manifest_client::{SubgraphInfo, SubgraphInfoMap},
    metrics::{with_metric, METRICS},
    receipts::{ReceiptPools, ReceiptStatus},
    reports,
    subgraph_deployments::SubgraphDeployments,
    unattestable_errors::{
        MISCATEGORIZED_ATTESTABLE_ERROR_MESSAGE_FRAGMENTS, UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS,
    },
};
use actix_http::{
    header::{AUTHORIZATION, ORIGIN},
    StatusCode,
};
use actix_web::{http::header, web, HttpRequest, HttpResponse, HttpResponseBuilder};
use futures::future::join_all;
use indexer_selection::{
    actor::Update, BlockRequirements, Context as AgoraContext,
    IndexerError as IndexerSelectionError, IndexerErrorObservation, Indexing, InputError,
    Selection, UnresolvedBlock, UtilityParameters, SELECTION_LIMIT,
};
use lazy_static::lazy_static;
use prelude::{
    anyhow::{anyhow, bail, Context as _},
    buffer_queue::QueueWriter,
    double_buffer::DoubleBufferReader,
    graphql::{
        graphql_parser::query::{OperationDefinition, SelectionSet},
        http::Response,
    },
    url::Url,
    DeploymentId, Eventual, *,
};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};
use uuid::Uuid;

fn query_id() -> String {
    lazy_static! {
        static ref GATEWAY_ID: Uuid = Uuid::new_v4();
    }
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let local_id = COUNTER.fetch_add(1, atomic::Ordering::Relaxed);
    format!("{}-{:x}", *GATEWAY_ID, local_id)
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Block not found: {0}")]
    BlockNotFound(UnresolvedBlock),
    #[error("Subgraph deployment not found (subgraph migrated to L2): {0}")]
    DeploymentMigrated(DeploymentId),
    #[error("Subgraph deployment not found: {0}")]
    DeploymentNotFound(DeploymentId),
    #[error("Internal error: {0:#}")]
    Internal(anyhow::Error),
    #[error("{0:#}")]
    InvalidAuth(anyhow::Error),
    #[error("Invalid subgraph deployment: {0}")]
    InvalidDeploymentId(String),
    #[error("Invalid query: {0:#}")]
    InvalidQuery(anyhow::Error),
    #[error("Invalid subgraph: {0}")]
    InvalidSubgraphId(String),
    #[error("No indexers found for subgraph deployment")]
    NoIndexers,
    #[error("No suitable indexer found for subgraph deployment. {0:#}")]
    NoSuitableIndexer(anyhow::Error),
    #[error("Subgraph chain not supported: {0}")]
    SubgraphChainNotSupported(String),
    #[error("Subgraph not found: {0}")]
    SubgraphNotFound(SubgraphId),
}

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub fisherman_client: Option<&'static FishermanClient>,
    pub graph_env_id: String,
    pub auth_handler: &'static AuthHandler,
    pub indexer_selection_retry_limit: usize,
    pub block_caches: &'static HashMap<String, BlockCache>,
    pub subgraph_info: SubgraphInfoMap,
    pub subgraph_deployments: SubgraphDeployments,
    pub deployment_indexers: Eventual<Ptr<HashMap<DeploymentId, Vec<Address>>>>,
    pub receipt_pools: &'static ReceiptPools,
    pub isa_state: DoubleBufferReader<indexer_selection::State>,
    pub observations: QueueWriter<Update>,
}

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    pub query: String,
    pub variables: Option<Box<RawValue>>,
}

pub async fn handle_query(
    request: HttpRequest,
    payload: web::Json<QueryBody>,
    ctx: web::Data<Context>,
) -> HttpResponse {
    let start_time_ms = unix_timestamp();
    let headers = request.headers();
    let ray_id = headers.get("cf-ray").and_then(|value| value.to_str().ok());
    let query_id = ray_id.map(ToString::to_string).unwrap_or_else(query_id);

    let auth = match (
        request.match_info().get("api_key"),
        headers.get(AUTHORIZATION).and_then(|h| h.to_str().ok()),
    ) {
        (Some(param), _) => param,
        (None, Some(header)) => header.trim_start_matches("Bearer").trim(),
        (None, None) => "",
    };
    tracing::debug!(%auth);
    let auth = ctx
        .auth_handler
        .parse_token(auth)
        .context("Invalid API key");

    let subgraph_resolution_result = resolve_subgraph_deployment(
        &ctx.subgraph_deployments,
        &ctx.subgraph_info,
        request.match_info(),
    )
    .await;
    let deployment = subgraph_resolution_result
        .as_ref()
        .map(|subgraph_info| subgraph_info.deployment.to_string())
        .ok();
    let span = tracing::info_span!(
        target: reports::CLIENT_QUERY_TARGET,
        "client_query",
        %query_id,
        graph_env = %ctx.graph_env_id,
        deployment,
    );

    let domain = headers
        .get(ORIGIN)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Some(v.parse::<Url>().ok()?.host_str()?.to_string()))
        .unwrap_or("".to_string());

    let result = match (auth, subgraph_resolution_result) {
        (Ok(auth), Ok(subgraph_info)) => {
            handle_client_query_inner(&ctx, subgraph_info, payload.0, auth, domain)
                .instrument(span.clone())
                .await
        }
        (Err(auth_err), _) => Err(Error::InvalidAuth(auth_err)),
        (_, Err(subgraph_resolution_err)) => Err(subgraph_resolution_err),
    };
    METRICS
        .client_query
        .check(&[deployment.as_deref().unwrap_or("")], &result);

    span.in_scope(|| {
        let (status_message, status_code) = reports::status(&result);
        let (legacy_status_message, legacy_status_code) = reports::legacy_status(&result);
        tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            start_time_ms,
            %status_message,
            status_code,
            %legacy_status_message,
            legacy_status_code,
        );
    });

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
        Err(err) => graphql_error_response(err.to_string()),
    }
}

async fn resolve_subgraph_deployment(
    deployments: &SubgraphDeployments,
    subgraph_info: &SubgraphInfoMap,
    params: &actix_web::dev::Path<actix_web::dev::Url>,
) -> Result<Ptr<SubgraphInfo>, Error> {
    let deployment = if let Some(id) = params.get("subgraph_id") {
        let subgraph = id
            .parse::<SubgraphId>()
            .map_err(|_| Error::InvalidSubgraphId(id.to_string()))?;
        deployments
            .current_deployment(&subgraph)
            .await
            .ok_or_else(|| Error::SubgraphNotFound(subgraph))?
    } else if let Some(id) = params.get("deployment_id") {
        DeploymentId::from_ipfs_hash(id)
            .ok_or_else(|| Error::InvalidDeploymentId(id.to_string()))?
    } else {
        return Err(Error::InvalidDeploymentId("".to_string()));
    };
    if deployments.migrated_away(&deployment).await {
        return Err(Error::DeploymentMigrated(deployment));
    }
    subgraph_info
        .value_immediate()
        .and_then(|map| map.get(&deployment)?.value_immediate())
        .ok_or_else(|| Error::DeploymentNotFound(deployment))
}

async fn handle_client_query_inner(
    ctx: &Context,
    subgraph_info: Ptr<SubgraphInfo>,
    payload: QueryBody,
    auth: AuthToken,
    domain: String,
) -> Result<ResponsePayload, Error> {
    let deployment = subgraph_info.deployment.to_string();
    let _timer = METRICS.client_query.start_timer(&[&deployment]);

    tracing::info!(
        target: reports::CLIENT_QUERY_TARGET,
        subgraph_chain = %subgraph_info.network,
    );
    match &auth {
        AuthToken::ApiKey(api_key) => tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            api_key = %api_key.key,
        ),
        AuthToken::Ticket(payload, _) => tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            ticket_user = ?payload.user.unwrap_or(payload.signer),
            ticket_signer = ?payload.signer,
            ticket_name = payload.name,
        ),
    };

    ctx.auth_handler
        .check_token(&auth, &subgraph_info, &domain)
        .await
        .map_err(Error::InvalidAuth)?;

    let deployment_indexers = ctx
        .deployment_indexers
        .value_immediate()
        .and_then(|map| map.get(&subgraph_info.deployment).cloned())
        .unwrap_or_default();
    tracing::info!(deployment_indexers = deployment_indexers.len());
    if deployment_indexers.is_empty() {
        return Err(Error::NoIndexers);
    }

    let variables = payload
        .variables
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let context = AgoraContext::new(&payload.query, &variables)
        .map_err(|err| Error::InvalidQuery(anyhow!("{}", err)))?;

    tracing::info!(
        target: reports::CLIENT_QUERY_TARGET,
        query = %payload.query,
        %variables,
    );

    let mut block_cache = ctx
        .block_caches
        .get(&subgraph_info.network)
        .cloned()
        .ok_or_else(|| Error::SubgraphChainNotSupported(subgraph_info.network.clone()))?;

    let block_constraints = block_constraints(&context)
        .ok_or_else(|| Error::InvalidQuery(anyhow!("Failed to determine block constraints.")))?;
    let resolved_blocks = join_all(
        block_constraints
            .iter()
            .filter_map(|constraint| constraint.clone().into_unresolved())
            .map(|unresolved| block_cache.fetch_block(unresolved)),
    )
    .await
    .into_iter()
    .collect::<Result<BTreeSet<BlockPointer>, UnresolvedBlock>>()
    .map_err(Error::BlockNotFound)?;
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
    if matches!(min_block, Some(min_block) if min_block < subgraph_info.min_block) {
        return Err(Error::InvalidQuery(anyhow!(
            "Requested block before minimum `startBlock` of subgraph manifest: {}",
            min_block.unwrap_or_default()
        )));
    }

    let query_count = match &auth {
        AuthToken::Ticket(_, _) => count_top_level_selection_sets(&context),
        // Maintain old (incorrect) behavior for studio API keys. This is not consistent with how
        // Agora counts queries, but we want to avoid price shocks for now.
        AuthToken::ApiKey(_) => Ok(context.operations.len()),
    }
    .map_err(Error::InvalidQuery)?
    .max(1) as u64;

    let settings = ctx.auth_handler.query_settings(&auth, query_count).await;
    let budget: GRT = ctx
        .isa_state
        .latest()
        .network_params
        .usd_to_grt(settings.budget)
        .ok_or_else(|| Error::Internal(anyhow!("Missing exchange rate")))?;

    tracing::info!(
        target: reports::CLIENT_QUERY_TARGET,
        query_count,
        budget_grt = budget.as_f64() as f32,
    );

    let mut utility_params = UtilityParameters::new(
        budget,
        block_requirements,
        0, // 170cbcf3-db7f-404a-be13-2022d9142677
        block_cache.block_rate_hz,
        settings.indexer_preferences.performance,
        settings.indexer_preferences.data_freshness,
        settings.indexer_preferences.economic_security,
        settings.indexer_preferences.price_efficiency,
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
            .ok_or(Error::BlockNotFound(UnresolvedBlock::WithNumber(0)))?;
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
            &[&deployment],
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
                InputError::MalformedQuery => Error::InvalidQuery(anyhow!("Failed to parse")),
                InputError::MissingNetworkParams => {
                    Error::Internal(anyhow!("Missing network params"))
                }
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
            return Err(Error::NoSuitableIndexer(anyhow!(
                "Indexer selection errors: {:?}",
                isa_errors
            )));
        }

        tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            indexer_fees_grt = selections
                .iter()
                .map(|s| &s.fee)
                .fold(GRT::zero(), |sum, fee| sum + *fee)
                .as_f64() as f32,
        );

        let indexer_query_context = IndexerQueryContext {
            indexer_client: ctx.indexer_client.clone(),
            fisherman_client: ctx.fisherman_client,
            receipt_pools: ctx.receipt_pools,
            observations: ctx.observations.clone(),
            subgraph_info: subgraph_info.clone(),
            latest_block: latest_block.number,
            response_time: Duration::default(),
            subgraph_chain: subgraph_info.network.clone(),
        };

        let (response_tx, mut response_rx) = mpsc::channel(SELECTION_LIMIT);
        for selection in selections {
            let latest_query_block = match block_cache
                .latest(selection.blocks_behind + latest_unresolved)
                .await
            {
                Ok(latest_query_block) => latest_query_block,
                Err(_) if !last_retry => continue,
                Err(unresolved) => return Err(Error::BlockNotFound(unresolved)),
            };
            let deterministic_query =
                make_query_deterministic(context.clone(), &resolved_blocks, &latest_query_block)
                    .ok_or_else(|| {
                        Error::InvalidQuery(anyhow!("Failed to set block constraints"))
                    })?;

            let indexer_query_context = indexer_query_context.clone();
            let response_tx = response_tx.clone();
            tokio::spawn(
                async move {
                    let response = handle_indexer_query(
                        indexer_query_context,
                        selection,
                        deterministic_query,
                        latest_query_block.number,
                    )
                    .await;
                    let _ = response_tx.try_send(response);
                }
                .in_current_span(),
            );
        }
        for _ in 0..selections_len {
            match response_rx.recv().await {
                Some(Ok(payload)) => return Ok(payload),
                Some(Err(IndexerError::UnresolvedBlock)) => latest_unresolved += 1,
                Some(Err(_)) | None => (),
            };
        }
    }

    Err(Error::NoSuitableIndexer(anyhow!(
        "Indexer queries attempted: {}",
        total_indexer_queries
    )))
}

#[derive(Clone)]
struct IndexerQueryContext {
    pub indexer_client: IndexerClient,
    pub fisherman_client: Option<&'static FishermanClient>,
    pub receipt_pools: &'static ReceiptPools,
    pub observations: QueueWriter<Update>,
    pub subgraph_info: Ptr<SubgraphInfo>,
    pub latest_block: u64,
    pub response_time: Duration,
    pub subgraph_chain: String,
}

#[tracing::instrument(
    target = "indexer_query", name = "indexer_query",
    skip_all, fields(indexer = %selection.indexing.indexer),
)]
async fn handle_indexer_query(
    mut ctx: IndexerQueryContext,
    selection: Selection,
    deterministic_query: String,
    latest_query_block: u64,
) -> Result<ResponsePayload, IndexerError> {
    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        url = %selection.url,
        blocks_behind = selection.blocks_behind,
        fee_grt = selection.fee.as_f64() as f32,
        subgraph_chain = %ctx.subgraph_chain,
    );

    let indexing = selection.indexing;
    let deployment = indexing.deployment.to_string();

    let receipt = ctx
        .receipt_pools
        .commit(&selection.indexing, selection.fee)
        .await
        .map_err(|_| IndexerError::NoAllocation);

    let result = match receipt.as_ref() {
        Ok(receipt) => {
            handle_indexer_query_inner(&mut ctx, selection, deterministic_query, receipt).await
        }
        Err(err) => Err(err.clone()),
    };
    METRICS.indexer_query.check(&[&deployment], &result);

    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        response_time_ms = ctx.response_time.as_millis() as u32,
        status_message = match &result {
            Ok(_) => "200 OK".to_string(),
            Err(err) => format!("{err:?}"),
        },
        status_code = reports::indexer_attempt_status_code(&result),
    );

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
        duration: ctx.response_time,
        result: observation,
    });

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
    ctx.response_time = Instant::now() - start_time;
    let deployment = selection.indexing.deployment.to_string();
    with_metric(&METRICS.indexer_query.duration, &[&deployment], |hist| {
        hist.observe(ctx.response_time.as_millis() as f64)
    });

    let mut allocation = Address([0; 20]);
    allocation.0.copy_from_slice(&receipt[0..20]);

    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        allocation = allocation.to_string(),
    );

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

    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        indexer_errors = indexer_errors.join(","),
    );

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

    // Return early if we aren't expecting an attestation.
    if !ctx.subgraph_info.features.is_empty() {
        return Ok(response.payload);
    }

    if response.payload.attestation.is_none() {
        // TODO: This is a temporary hack to handle errors that were previously miscategorized as
        // unattestable in graph-node.
        for error in &indexer_errors {
            if MISCATEGORIZED_ATTESTABLE_ERROR_MESSAGE_FRAGMENTS
                .iter()
                .any(|err| error.contains(err))
            {
                return Ok(response.payload);
            }
        }

        return Err(IndexerError::NoAttestation);
    }

    if let Some(attestation) = &response.payload.attestation {
        challenge_indexer_response(
            ctx.fisherman_client,
            ctx.observations.clone(),
            selection.indexing,
            allocation,
            deterministic_query,
            response.payload.body.clone(),
            attestation.clone(),
        );
    }

    Ok(response.payload)
}

fn challenge_indexer_response(
    fisherman_client: Option<&'static FishermanClient>,
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

fn count_top_level_selection_sets(ctx: &AgoraContext) -> anyhow::Result<usize> {
    let selection_sets = ctx
        .operations
        .iter()
        .map(|op| match op {
            OperationDefinition::SelectionSet(selection_set) => Ok(selection_set),
            OperationDefinition::Query(query) => Ok(&query.selection_set),
            OperationDefinition::Mutation(_) => bail!("Mutations not yet supported"),
            OperationDefinition::Subscription(_) => bail!("Subscriptions not yet supported"),
        })
        .collect::<anyhow::Result<Vec<&SelectionSet<String>>>>()?;
    Ok(selection_sets.into_iter().map(|set| set.items.len()).sum())
}
