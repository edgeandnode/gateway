use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

use axum::extract::OriginalUri;
use axum::http::{HeaderValue, Request, Uri};
use axum::middleware::Next;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, Response, StatusCode},
    RequestPartsExt,
};
use futures::future::join_all;
use lazy_static::lazy_static;
use prost::bytes::Buf;
use semver::Version;
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use uuid::Uuid;

use indexer_selection::{
    actor::Update, BlockRequirements, Candidate, Context as AgoraContext,
    IndexerError as IndexerSelectionError, IndexerErrorObservation, Indexing, InputError,
    Selection, UnresolvedBlock, UtilityParameters, SELECTION_LIMIT,
};
use prelude::{
    anyhow::{anyhow, bail, Context as _},
    buffer_queue::QueueWriter,
    double_buffer::DoubleBufferReader,
    graphql::graphql_parser::query::{OperationDefinition, SelectionSet},
    url::Url,
    DeploymentId, *,
};

use crate::{
    auth::{AuthHandler, AuthToken},
    block_constraints::{block_constraints, make_query_deterministic, BlockConstraint},
    chains::BlockCache,
    fisherman_client::{ChallengeOutcome, FishermanClient},
    indexer_client::{Attestation, IndexerClient, IndexerError, ResponsePayload},
    metrics::{with_metric, METRICS},
    receipts::{ReceiptPools, ReceiptStatus},
    reports,
    topology::{Deployment, GraphNetwork, Subgraph},
    unattestable_errors::{
        MISCATEGORIZED_ATTESTABLE_ERROR_MESSAGE_FRAGMENTS, UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS,
    },
};

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
    InvalidSubgraph(String),
    #[error("No indexers found for subgraph deployment")]
    NoIndexers,
    #[error("No suitable indexer found for subgraph deployment. {0:#}")]
    NoSuitableIndexer(anyhow::Error),
    #[error("Subgraph chain not supported: {0}")]
    SubgraphChainNotSupported(String),
    #[error("Subgraph not found: {0}")]
    SubgraphNotFound(SubgraphId),
    #[error("L2 gateway request failed: {0}")]
    L2GatewayError(anyhow::Error),
}

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub fisherman_client: Option<&'static FishermanClient>,
    pub graph_env_id: String,
    pub auth_handler: &'static AuthHandler,
    pub indexer_selection_retry_limit: usize,
    pub l2_gateway: Option<Url>,
    pub block_caches: &'static HashMap<String, BlockCache>,
    pub network: GraphNetwork,
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
    State(ctx): State<Context>,
    OriginalUri(original_uri): OriginalUri,
    Path(params): Path<BTreeMap<String, String>>,
    headers: HeaderMap,
    payload: Bytes,
) -> Response<String> {
    let start_time = Instant::now();
    let timestamp = unix_timestamp();
    let ray_id = headers.get("cf-ray").and_then(|value| value.to_str().ok());
    let query_id = ray_id.map(ToString::to_string).unwrap_or_else(query_id);

    let auth_input = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .map(|header| header.trim_start_matches("Bearer").trim())
        .unwrap_or("");
    let auth = ctx
        .auth_handler
        .parse_token(auth_input)
        .context("Invalid auth");

    let resolved_deployments = resolve_subgraph_deployments(&ctx.network, &params).await;
    // Forward query to L2 gateway if subgraph has transferred to L2. On deployment queries, only
    // forward when deployments also have no allocations. We only resolve a subgraph when a subgraph
    // ID is given as a URL param.
    let subgraph = resolved_deployments
        .as_ref()
        .ok()
        .and_then(|(_, s)| s.as_ref());
    let l2_subgraph_id = subgraph.and_then(|s| s.l2_id);
    let deployments_migrated = matches!(
        resolved_deployments.as_ref(),
        Ok((deployments, _)) if deployments.iter().all(|d| d.transferred_to_l2),
    );
    if l2_subgraph_id.is_some() || (subgraph.is_none() && deployments_migrated) {
        // We validate the configuration correctness at startup: L2 transfer redirection requires
        // the L2 gateway URL to be configured.
        // be8f0ed1-262e-426f-877a-613368eed3ca
        let l2_url = ctx.l2_gateway.as_ref().unwrap();
        return forward_request_to_l2(
            &ctx.indexer_client.client,
            l2_url,
            &original_uri,
            headers,
            payload,
            l2_subgraph_id,
        )
        .await;
    }

    let span = tracing::info_span!(
        target: reports::CLIENT_QUERY_TARGET,
        "client_query",
        %query_id,
        graph_env = %ctx.graph_env_id,
    );
    span.in_scope(|| {
        tracing::debug!(%auth_input);
    });

    let domain = headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Some(v.parse::<Url>().ok()?.host_str()?.to_string()))
        .unwrap_or("".to_string());

    let result = match (auth, resolved_deployments) {
        (Ok(auth), Ok((deployments, _))) => {
            handle_client_query_inner(&ctx, deployments, payload, auth, domain)
                .instrument(span.clone())
                .await
        }
        (Err(auth_err), _) => Err(Error::InvalidAuth(auth_err)),
        (_, Err(subgraph_resolution_err)) => Err(subgraph_resolution_err),
    };

    let deployment: Option<String> = result
        .as_ref()
        .map(|response| response.selection.indexing.deployment.to_string())
        .ok();
    let metric_labels = [deployment.as_deref().unwrap_or("")];

    METRICS.client_query.check(&metric_labels, &result);
    with_metric(&METRICS.client_query.duration, &metric_labels, |h| {
        h.observe((Instant::now() - start_time).as_secs_f64())
    });

    span.in_scope(|| {
        let (status_message, status_code) = reports::status(&result);
        let (legacy_status_message, legacy_status_code) = reports::legacy_status(&result);
        tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            start_time_ms = timestamp,
            deployment,
            %status_message,
            status_code,
            %legacy_status_message,
            legacy_status_code,
        );
    });

    match result {
        Ok(QueryOutcome {
            response: ResponsePayload { body, attestation },
            ..
        }) => {
            let attestation = attestation
                .as_ref()
                .and_then(|attestation| serde_json::to_string(attestation).ok())
                .unwrap_or_default();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Graph-Attestation", attestation)
                .body(body.to_string())
                .unwrap()
        }
        Err(err) => error_response(err),
    }
}

async fn forward_request_to_l2(
    client: &reqwest::Client,
    l2_url: &Url,
    original_uri: &Uri,
    headers: HeaderMap,
    payload: Bytes,
    l2_subgraph_id: Option<SubgraphId>,
) -> Response<String> {
    let request = rebuild_request_for_l2(
        client,
        l2_url,
        original_uri,
        headers,
        payload,
        l2_subgraph_id,
    );
    let response = match client
        .execute(request)
        .await
        .and_then(|response| response.error_for_status())
    {
        Ok(response) => response,
        Err(err) => return error_response(Error::L2GatewayError(err.into())),
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.text().await {
        Ok(body) => body,
        Err(err) => return error_response(Error::L2GatewayError(err.into())),
    };
    let mut response = Response::builder().status(status);
    {
        let headers_mut = response.headers_mut().unwrap();
        *headers_mut = headers;
    }
    response.body(body).unwrap()
}

fn rebuild_request_for_l2(
    client: &reqwest::Client,
    l2_url: &Url,
    original_uri: &Uri,
    headers: HeaderMap,
    payload: Bytes,
    l2_subgraph_id: Option<SubgraphId>,
) -> reqwest::Request {
    let uri = {
        let mut builder = Uri::builder();
        builder = builder.scheme(l2_url.scheme());
        if let Some(host) = l2_url.host_str() {
            builder = builder.authority(host);
        }
        let mut path = original_uri.path().to_string();

        // rewrite path of subgraph queries to the L2 subgraph ID.
        if let (Some(l2_subgraph_id), Some(replace_start)) =
            (l2_subgraph_id, path.find("subgraphs/id/"))
        {
            let replace_start = replace_start + "subgraphs/id/".len();
            let replace_end = path.find('^').unwrap_or(path.len());
            path.replace_range(replace_start..replace_end, &l2_subgraph_id.to_string());
        }

        // Note: original query fragment is dropped.
        builder.path_and_query(path).build().unwrap()
    };
    tracing::info!(l2_forward_uri = %uri, %original_uri);

    client
        .post(uri.to_string())
        .headers(headers)
        .body(payload)
        .build()
        .unwrap()
}

fn graphql_error_response<S: ToString>(message: S) -> String {
    let json_error = json!({"errors": [{"message": message.to_string()}]});
    serde_json::to_string(&json_error).expect("failed to serialize error response")
}

fn error_response(err: Error) -> Response<String> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(graphql_error_response(err))
        .unwrap()
}

async fn resolve_subgraph_deployments(
    network: &GraphNetwork,
    params: &BTreeMap<String, String>,
) -> Result<(Vec<Arc<Deployment>>, Option<Subgraph>), Error> {
    if let Some(constraint) = params.get("subgraph_id") {
        let (id, comparator) = constraint
            .split_once('^')
            .map(|(id, comparator)| (id, Some(comparator)))
            .unwrap_or((constraint, None));
        let id = SubgraphId::from_str(id).map_err(|_| Error::InvalidSubgraph(id.to_string()))?;
        let subgraph = network
            .subgraphs
            .value_immediate()
            .and_then(|subgraphs| subgraphs.get(&id).cloned())
            .ok_or_else(|| Error::SubgraphNotFound(id))?;
        let comparator = match comparator {
            None => None,
            Some(comparator) => Some(
                semver::Comparator::from_str(comparator)
                    .map_err(|err| Error::InvalidSubgraph(err.to_string()))?,
            ),
        };
        let versions = resolve_subgraph_versions(&subgraph, comparator)
            .ok_or_else(|| Error::InvalidSubgraph("No matching deployments".to_string()))?;
        Ok((versions, Some(subgraph)))
    } else if let Some(id) = params.get("deployment_id") {
        let id = DeploymentId::from_ipfs_hash(id)
            .ok_or_else(|| Error::InvalidDeploymentId(id.to_string()))?;
        network
            .deployments
            .value_immediate()
            .and_then(|deployments| Some((vec![deployments.get(&id)?.clone()], None)))
            .ok_or_else(|| Error::DeploymentNotFound(id))
    } else {
        Err(Error::InvalidDeploymentId("".to_string()))
    }
}

fn resolve_subgraph_versions(
    subgraph: &Subgraph,
    constraint: Option<semver::Comparator>,
) -> Option<Vec<Arc<Deployment>>> {
    let comparator = match constraint {
        Some(comparator) => comparator,
        None => {
            return subgraph
                .deployments
                .last()
                .map(|deployment| vec![deployment.clone()])
        }
    };
    let matching_subgraph_versions: Vec<Arc<Deployment>> = subgraph
        .deployments
        .iter()
        .filter(|deployment| match &deployment.version {
            None => false,
            Some(version) => comparator.matches(version),
        })
        .cloned()
        .collect();
    Some(matching_subgraph_versions)
}

struct QueryOutcome {
    response: ResponsePayload,
    selection: Selection,
}

async fn handle_client_query_inner(
    ctx: &Context,
    mut deployments: Vec<Arc<Deployment>>,
    payload: Bytes,
    auth: AuthToken,
    domain: String,
) -> Result<QueryOutcome, Error> {
    let subgraph_chain = deployments
        .last()
        .map(|deployment| deployment.manifest.network.clone())
        .ok_or_else(|| Error::InvalidSubgraph("No matching deployments".to_string()))?;
    tracing::info!(target: reports::CLIENT_QUERY_TARGET, subgraph_chain);
    // Make sure we only select from deployments indexing the same chain. This simplifies dealing
    // with block constraints later.
    deployments.retain(|deployment| deployment.manifest.network == subgraph_chain);
    tracing::info!(deployments = ?deployments.iter().map(|d| d.id).collect::<Vec<_>>());

    let manifest_min_block = deployments.last().unwrap().manifest.min_block;

    match &auth {
        AuthToken::ApiKey(api_key) => tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            user_address = %api_key.user_address,
            api_key = %api_key.key,
        ),
        AuthToken::Ticket(payload, _) => tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            user_address = ?payload.user(),
            ticket_payload = serde_json::to_string(payload).unwrap(),
        ),
    };

    let payload: QueryBody =
        serde_json::from_reader(payload.reader()).map_err(|err| Error::InvalidQuery(err.into()))?;

    ctx.auth_handler
        .check_token(&auth, &deployments, &domain)
        .await
        .map_err(Error::InvalidAuth)?;

    let available_indexings: Vec<Indexing> = deployments
        .iter()
        .flat_map(|deployment| {
            let id = deployment.id;
            deployment
                .allocations
                .iter()
                .map(move |allocation| Indexing {
                    indexer: allocation.indexer.id,
                    deployment: id,
                })
        })
        .collect::<BTreeSet<Indexing>>()
        .into_iter()
        .collect();
    tracing::info!(available_indexings = available_indexings.len());
    if available_indexings.is_empty() {
        return Err(Error::NoIndexers);
    }
    let candidates = indexings_to_candidates(&deployments, available_indexings);

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
        .get(&subgraph_chain)
        .cloned()
        .ok_or_else(|| Error::SubgraphChainNotSupported(subgraph_chain))?;

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

    // Reject queries for blocks before minimum start block in the manifest.
    if matches!(min_block, Some(min_block) if min_block < manifest_min_block) {
        return Err(Error::InvalidQuery(anyhow!(
            "Requested block before minimum `startBlock` of manifest: {}",
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

        let selection_timer = METRICS.indexer_selection_duration.start_timer();
        let (selections, indexer_errors) = ctx
            .isa_state
            .latest()
            .select_indexers(
                &candidates,
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

        let (outcome_tx, mut outcome_rx) = mpsc::channel(SELECTION_LIMIT);
        for selection in selections {
            let deployment = deployments
                .iter()
                .find(|deployment| deployment.id == selection.indexing.deployment)
                .unwrap()
                .clone();
            let indexer_query_context = IndexerQueryContext {
                indexer_client: ctx.indexer_client.clone(),
                fisherman_client: ctx.fisherman_client,
                receipt_pools: ctx.receipt_pools,
                observations: ctx.observations.clone(),
                deployment,
                latest_block: latest_block.number,
                response_time: Duration::default(),
            };

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
            let outcome_tx = outcome_tx.clone();
            // We must manually construct this span before the spawned task, since otherwise
            // there's a race between creating this span and another indexer responding which will
            // close the outer client_query span.
            let span = tracing::info_span!(
                target: reports::INDEXER_QUERY_TARGET,
                "indexer_query",
                indexer = %selection.indexing.indexer,
            );
            tokio::spawn(
                async move {
                    let response = handle_indexer_query(
                        indexer_query_context,
                        selection.clone(),
                        deterministic_query,
                        latest_query_block.number,
                    )
                    .await;
                    let _ = outcome_tx.try_send(response.map(|response| QueryOutcome {
                        response,
                        selection,
                    }));
                }
                .instrument(span),
            );
        }
        for _ in 0..selections_len {
            match outcome_rx.recv().await {
                Some(Ok(outcome)) => return Ok(outcome),
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
    pub deployment: Arc<Deployment>,
    pub latest_block: u64,
    pub response_time: Duration,
}

async fn handle_indexer_query(
    mut ctx: IndexerQueryContext,
    selection: Selection,
    deterministic_query: String,
    latest_query_block: u64,
) -> Result<ResponsePayload, IndexerError> {
    let indexing = selection.indexing;
    let deployment = indexing.deployment.to_string();

    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        %deployment,
        url = %selection.url,
        blocks_behind = selection.blocks_behind,
        fee_grt = selection.fee.as_f64() as f32,
        subgraph_chain = %ctx.deployment.manifest.network,
    );

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

    let indexer_errors =
        serde_json::from_str::<graphql::http::Response<Box<RawValue>>>(&response.payload.body)
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
    if !ctx.deployment.manifest.features.is_empty() {
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

fn indexings_to_candidates(
    deployments: &[Arc<Deployment>],
    indexings: Vec<Indexing>,
) -> Vec<Candidate> {
    let versions: BTreeMap<&Version, DeploymentId> = deployments
        .iter()
        .filter_map(|deployment| Some((deployment.version.as_ref()?.as_ref(), deployment.id)))
        .collect();
    let deployment_versions_behind: BTreeMap<DeploymentId, usize> = versions
        .into_iter()
        .rev()
        .enumerate()
        .map(|(index, (_, deployment))| (deployment, index))
        .collect();
    indexings
        .into_iter()
        .map(|indexing| {
            let versions_behind = *deployment_versions_behind
                .get(&indexing.deployment)
                .unwrap_or(&0)
                .min(&(u8::MAX as usize)) as u8;
            Candidate {
                indexing,
                versions_behind,
            }
        })
        .collect()
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

/// This adapter middleware extracts the authorization token from the `api_key` path parameter,
/// and adds it to the request in the `Authorization` header.
///
/// If the request already has an `Authorization` header, it is left unchanged.
/// If the request does not have an `api_key` path parameter, it is left unchanged.
///
/// This is a temporary adapter middleware to allow legacy clients to use the new auth scheme.
pub async fn legacy_auth_adapter<B>(
    request: Request<B>,
    next: Next<B>,
) -> axum::response::Response {
    // If the request already has an `Authorization` header, don't do anything
    if request.headers().contains_key(header::AUTHORIZATION) {
        return next.run(request).await;
    }

    let (mut parts, body) = request.into_parts();

    // Extract the `api_key` from the path and add it to the Authorization header
    if let Ok(Path(path)) = parts.extract::<Path<BTreeMap<String, String>>>().await {
        if let Some(api_key) = path.get("api_key") {
            parts.headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", api_key)).unwrap(),
            );
        }
    }

    // reconstruct the request
    let request = Request::from_parts(parts, body);

    next.run(request).await
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, sync::Arc};

    use crate::topology::{Deployment, Manifest, Subgraph};

    use super::*;

    #[test]
    fn resolving_subgraph_versions() {
        let deployment1 = "QmcvzjH2RvLiytkkwaiCB3fzkqzr33LbAh71nACB13UGr1"
            .parse()
            .unwrap();
        let deployment2 = "QmcvzjH2RvLiytkkwaiCB3fzkqzr33LbAh71nACB13UGr2"
            .parse()
            .unwrap();
        let deployment3 = "QmcvzjH2RvLiytkkwaiCB3fzkqzr33LbAh71nACB13UGr3"
            .parse()
            .unwrap();
        let deployment4 = "QmcvzjH2RvLiytkkwaiCB3fzkqzr33LbAh71nACB13UGr4"
            .parse()
            .unwrap();
        let deployment = |id: DeploymentId, version: Option<&str>| -> Arc<Deployment> {
            Arc::new(Deployment {
                id,
                manifest: Arc::new(Manifest {
                    network: "testnet".to_string(),
                    features: vec![],
                    min_block: 0,
                }),
                version: version.map(|v| Arc::new(v.parse().unwrap())),
                allocations: vec![],
                subgraphs: BTreeSet::new(),
                transferred_to_l2: false,
            })
        };
        let subgraph = Subgraph {
            deployments: vec![
                deployment(deployment1, Some("0.1.0")),
                deployment(deployment2, Some("0.2.0")),
                deployment(deployment3, Some("1.0.0")),
                deployment(deployment4, None),
            ],
            l2_id: None,
        };

        let tests: Vec<(Option<&str>, Vec<DeploymentId>)> = vec![
            (None, vec![deployment4]),
            (Some("^0"), vec![deployment1, deployment2]),
            (Some("^0.1"), vec![deployment1]),
            (Some("^0.2"), vec![deployment2]),
            (Some("^1"), vec![deployment3]),
        ];

        for (constraint, expected) in tests {
            let constraint = constraint.map(|c| c.parse().unwrap());
            let resolved: Vec<DeploymentId> = resolve_subgraph_versions(&subgraph, constraint)
                .unwrap()
                .into_iter()
                .map(|deployment| deployment.id)
                .collect();
            assert_eq!(resolved, expected);
        }
    }

    #[tokio::test]
    async fn rebuild_request_for_l2() {
        const L1_URL: &str = "https://gateway.thegraph.com";
        const L2_URL: &str = "https://gateway-arbitrum.thegraph.com";
        const DEPLOYMENT: &str = "QmdveVMs7nAvdBPxNoaMMAYgNcuSroneMctZDnZUgbPPP3";
        const L1_SUBGRAPH: &str = "EMRitnR1t3drKrDQSmJMSmHBPB2sGotgZE12DzWNezDn";
        const L2_SUBGRAPH: &str = "CVHoVSrdiiYvLcH4wocDCazJ1YuixHZ1SKt34UWmnQcC";
        let client = reqwest::Client::new();

        async fn check(
            original: &Request<Bytes>,
            expected: &reqwest::Request,
            l2_subgraph_id: Option<SubgraphId>,
        ) {
            let output = super::rebuild_request_for_l2(
                &reqwest::Client::new(),
                &L2_URL.parse().unwrap(),
                original.uri(),
                original.headers().clone(),
                original.body().clone(),
                l2_subgraph_id,
            );
            assert_eq!(output.url(), expected.url());
            assert_eq!(output.headers(), expected.headers());
            assert_eq!(
                output.body().unwrap().as_bytes(),
                expected.body().unwrap().as_bytes()
            );
        }

        fn update_test(
            original: &mut Request<Bytes>,
            original_replacement: String,
            expected: &mut reqwest::Request,
            expected_replacement: String,
        ) {
            let original_uri = original.uri_mut();
            *original_uri = original_replacement.try_into().unwrap();
            let expected_url = expected.url_mut();
            *expected_url = expected_replacement.parse().unwrap();
        }

        // test deployment route
        let mut original: Request<Bytes> = Request::builder()
            .uri(format!("{L1_URL}/api/deployments/id/{DEPLOYMENT}"))
            .header("Authorization", "Bearer deadbeefdeadbeefdeadbeefdeadbeef")
            .header("foo", "bar")
            .body("{}".into())
            .unwrap();
        let mut expected: reqwest::Request = client
            .post(format!("{L2_URL}/api/deployments/id/{DEPLOYMENT}"))
            .header("Authorization", "Bearer deadbeefdeadbeefdeadbeefdeadbeef")
            .header("foo", "bar")
            .body("{}")
            .build()
            .unwrap();
        check(&original, &expected, None).await;

        // test subgraph route
        update_test(
            &mut original,
            format!("{L1_URL}/api/subgraphs/id/{L1_SUBGRAPH}"),
            &mut expected,
            format!("{L2_URL}/api/subgraphs/id/{L2_SUBGRAPH}"),
        );
        check(&original, &expected, Some(L2_SUBGRAPH.parse().unwrap())).await;

        // test subgraph route with API key prefix
        update_test(
            &mut original,
            format!("{L1_URL}/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{L1_SUBGRAPH}"),
            &mut expected,
            format!("{L2_URL}/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{L2_SUBGRAPH}"),
        );
        check(&original, &expected, Some(L2_SUBGRAPH.parse().unwrap())).await;

        // test subgraph route with version constraint
        update_test(
            &mut original,
            format!("{L1_URL}/api/subgraphs/id/{L1_SUBGRAPH}^0.0.1"),
            &mut expected,
            format!("{L2_URL}/api/subgraphs/id/{L2_SUBGRAPH}^0.0.1"),
        );
        check(&original, &expected, Some(L2_SUBGRAPH.parse().unwrap())).await;
    }

    mod legacy_auth_adapter {
        use axum::body::Body;
        use axum::http::header::AUTHORIZATION;
        use axum::http::Method;
        use axum::routing::{get, post};
        use axum::{middleware, Router};
        use tower::ServiceExt;

        use super::*;

        fn test_router() -> Router {
            async fn handler(headers: HeaderMap) -> String {
                headers
                    .get(AUTHORIZATION)
                    .and_then(|header| header.to_str().ok())
                    .unwrap_or_default()
                    .to_owned()
            }

            let api = Router::new()
                .route("/subgraphs/id/:subgraph_id", post(handler))
                .route("/:api_key/subgraphs/id/:subgraph_id", post(handler))
                .route("/deployments/id/:deployment_id", post(handler))
                .route("/:api_key/deployments/id/:deployment_id", post(handler))
                .layer(middleware::from_fn(legacy_auth_adapter));
            Router::new()
                .route("/", get(|| async { "OK" }))
                .nest("/api", api)
        }

        fn test_body() -> Body {
            Body::from("test")
        }

        #[tokio::test]
        async fn test_preexistent_auth_header() {
            // Given
            let app = test_router();

            let api_key = "deadbeefdeadbeefdeadbeefdeadbeef"; // 32 hex digits
            let auth_header = format!("Bearer {api_key}");

            let request = Request::builder()
                .method(Method::POST)
                .uri("/api/subgraphs/id/456")
                .header(AUTHORIZATION, auth_header.clone())
                .body(test_body())
                .unwrap();

            // When
            let res = app.oneshot(request).await.unwrap();

            // Then
            assert_eq!(res.status(), StatusCode::OK);

            let body = hyper::body::to_bytes(res).await.unwrap();
            assert_eq!(&body[..], auth_header.as_bytes());
        }

        #[tokio::test]
        async fn test_legacy_auth() {
            // Given
            let app = test_router();

            let api_key = "deadbeefdeadbeefdeadbeefdeadbeef"; // 32 hex digits
            let auth_header = format!("Bearer {api_key}");

            let request = Request::builder()
                .method(Method::POST)
                .uri(format!("/api/{api_key}/subgraphs/id/456"))
                .body(test_body())
                .unwrap();

            // When
            let res = app.oneshot(request).await.unwrap();

            // Then
            assert_eq!(res.status(), StatusCode::OK);

            let body = hyper::body::to_bytes(res).await.unwrap();
            assert_eq!(&body[..], auth_header.as_bytes());
        }

        #[tokio::test]
        async fn test_legacy_auth_with_preexistent_auth_header() {
            // Given
            let app = test_router();

            let api_key = "deadbeefdeadbeefdeadbeefdeadbeef"; // 32 hex digits
            let auth_header = "Bearer 123";

            let request = Request::builder()
                .method(Method::POST)
                .uri(format!("/api/{api_key}/subgraphs/id/456"))
                .header(AUTHORIZATION, auth_header)
                .body(test_body())
                .unwrap();

            // When
            let res = app.oneshot(request).await.unwrap();

            // Then
            assert_eq!(res.status(), StatusCode::OK);

            let body = hyper::body::to_bytes(res).await.unwrap();
            assert_eq!(&body[..], auth_header.as_bytes());
        }

        #[tokio::test]
        async fn test_no_auth() {
            // Given
            let app = test_router();

            let request = Request::builder()
                .method(Method::POST)
                .uri("/api/subgraphs/id/456")
                .body(test_body())
                .unwrap();

            // When
            let res = app.oneshot(request).await.unwrap();

            // Then
            assert_eq!(res.status(), StatusCode::OK);

            let body = hyper::body::to_bytes(res).await.unwrap();
            assert_eq!(&body[..], b"");
        }
    }
}
