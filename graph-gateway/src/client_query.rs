use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, BlockNumber, U256};
use alloy_sol_types::Eip712Domain;
use anyhow::{anyhow, bail, Context as _};
use axum::extract::OriginalUri;
use axum::http::{HeaderValue, Request, Uri};
use axum::middleware::Next;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, Response, StatusCode},
    RequestPartsExt,
};
use cost_model::{Context as AgoraContext, CostModel};
use eventuals::{Eventual, Ptr};
use futures::future::join_all;
use graphql::graphql_parser::query::{OperationDefinition, SelectionSet};
use indexer_selection::Candidate;
use indexer_selection::{
    actor::Update, BlockRequirements, IndexerError as SelectionError, IndexerErrorObservation,
    Indexing, InputError, Selection, UnresolvedBlock, UtilityParameters, SELECTION_LIMIT,
};
use lazy_static::lazy_static;
use prelude::{buffer_queue::QueueWriter, double_buffer::DoubleBufferReader, unix_timestamp, GRT};
use prelude::{UDecimal18, USD};
use prost::bytes::Buf;
use rand::{rngs::SmallRng, SeedableRng as _};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use thegraph::types::{attestation, BlockPointer, DeploymentId, SubgraphId};
use tokio::sync::mpsc;
use toolshed::url::Url;
use tracing::Instrument;
use uuid::Uuid;

use crate::auth::{AuthHandler, AuthToken};
use crate::block_constraints::{block_constraints, make_query_deterministic, BlockConstraint};
use crate::budgets::Budgeter;
use crate::chains::BlockCache;
use crate::errors::{Error, IndexerError, IndexerErrors, UnavailableReason::*};
use crate::indexer_client::{check_block_error, IndexerClient, ResponsePayload};
use crate::indexers::indexing;
use crate::metrics::{with_metric, METRICS};
use crate::reports::{self, serialize_attestation, KafkaClient};
use crate::topology::{Deployment, GraphNetwork, Subgraph};
use crate::unattestable_errors::{miscategorized_attestable, miscategorized_unattestable};

fn query_id() -> String {
    lazy_static! {
        static ref GATEWAY_ID: Uuid = Uuid::new_v4();
    }
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let local_id = COUNTER.fetch_add(1, atomic::Ordering::Relaxed);
    format!("{}-{:x}", *GATEWAY_ID, local_id)
}

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub kafka_client: &'static KafkaClient,
    pub graph_env_id: String,
    pub auth_handler: &'static AuthHandler,
    pub budgeter: &'static Budgeter,
    pub indexer_selection_retry_limit: usize,
    pub l2_gateway: Option<Url>,
    pub block_caches: &'static HashMap<String, &'static BlockCache>,
    pub network: GraphNetwork,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, indexing::Status>>>,
    pub attestation_domain: &'static Eip712Domain,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
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
    // We only resolve a subgraph when a subgraph ID is given as a URL param.
    let subgraph = resolved_deployments
        .as_ref()
        .ok()
        .and_then(|(_, s)| s.as_ref());

    if let Some(l2_url) = ctx.l2_gateway.as_ref() {
        // Forward query to L2 gateway if it's marked as transferred & there are no allocations.
        // abf62a6d-c071-4507-b528-ddc8e250127a
        let transferred_to_l2 = matches!(
            resolved_deployments.as_ref(),
            Ok((deployments, _)) if deployments.iter().all(|d| d.transferred_to_l2),
        );
        if transferred_to_l2 {
            return forward_request_to_l2(
                &ctx.indexer_client.client,
                l2_url,
                &original_uri,
                headers,
                payload,
                subgraph.and_then(|s| s.l2_id),
            )
            .await;
        }
    }

    // This is very useful for investigating gateway logs in production.
    let selector = match &resolved_deployments {
        Ok((_, Some(subgraph))) => subgraph.id.to_string(),
        Ok((deployments, None)) => deployments
            .iter()
            .map(|d| d.id.to_string())
            .collect::<Vec<_>>()
            .join(","),
        Err(_) => "".to_string(),
    };
    let span = tracing::info_span!(
        target: reports::CLIENT_QUERY_TARGET,
        "client_query",
        %query_id,
        graph_env = %ctx.graph_env_id,
        selector,
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
        (Err(auth_err), _) => Err(Error::Auth(auth_err)),
        (_, Err(subgraph_resolution_err)) => Err(subgraph_resolution_err),
    };

    let deployment: Option<String> = result
        .as_ref()
        .map(|(selection, _)| selection.indexing.deployment.to_string())
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
        Ok((_, ResponsePayload { body, attestation })) => {
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
    original_path: &Uri,
    headers: HeaderMap,
    payload: Bytes,
    l2_subgraph_id: Option<SubgraphId>,
) -> Response<String> {
    // We originally attempted to proxy the user's request, but that resulted in a lot of strange
    // behavior from Cloudflare that was too difficult to debug.
    let l2_path = l2_request_path(original_path, l2_subgraph_id);
    let l2_url = l2_url.join(&l2_path).unwrap();
    tracing::info!(%l2_url, %original_path);
    let headers = headers
        .into_iter()
        .filter_map(|(k, v)| Some((k?, v)))
        .filter(|(k, _)| [header::CONTENT_TYPE, header::AUTHORIZATION, header::ORIGIN].contains(k))
        .collect();
    let response = match client
        .post(l2_url)
        .headers(headers)
        .body(payload)
        .send()
        .await
        .and_then(|response| response.error_for_status())
    {
        Ok(response) => response,
        Err(err) => return error_response(Error::Internal(anyhow!("L2 gateway error: {err}"))),
    };
    let status = response.status();
    if !status.is_success() {
        return error_response(Error::Internal(anyhow!("L2 gateway error: {status}")));
    }
    let body = match response.text().await {
        Ok(body) => body,
        Err(err) => return error_response(Error::Internal(anyhow!("L2 gateway error: {err}"))),
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}

fn l2_request_path(original_path: &Uri, l2_subgraph_id: Option<SubgraphId>) -> String {
    let mut path = original_path.path().to_string();
    let subgraph_prefix = "subgraphs/id/";
    let subgraph_start = path.find(subgraph_prefix);
    // rewrite path of subgraph queries to the L2 subgraph ID, conserving version constraint
    if let (Some(l2_subgraph_id), Some(replace_start)) = (l2_subgraph_id, subgraph_start) {
        let replace_start = replace_start + subgraph_prefix.len();
        let replace_end = path.find('^').unwrap_or(path.len());
        path.replace_range(replace_start..replace_end, &l2_subgraph_id.to_string());
    }
    path
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
    if let Some(id) = params.get("subgraph_id") {
        let id = SubgraphId::from_str(id)
            .map_err(|_| Error::SubgraphNotFound(anyhow!("invalid subgraph ID: {id}")))?;
        let subgraph = network
            .subgraphs
            .value_immediate()
            .and_then(|subgraphs| subgraphs.get(&id).cloned())
            .ok_or_else(|| Error::SubgraphNotFound(anyhow!("{id}")))?;
        let versions = subgraph.deployments.clone();
        Ok((versions, Some(subgraph)))
    } else if let Some(id) = params.get("deployment_id") {
        let id: DeploymentId = id
            .parse()
            .map_err(|_| Error::SubgraphNotFound(anyhow!("invalid deployment ID: {id}")))?;
        network
            .deployments
            .value_immediate()
            .and_then(|deployments| Some((vec![deployments.get(&id)?.clone()], None)))
            .ok_or_else(|| Error::SubgraphNotFound(anyhow!("deployment not found: {id}")))
    } else {
        Err(Error::SubgraphNotFound(anyhow!("missing identifier")))
    }
}

async fn handle_client_query_inner(
    ctx: &Context,
    mut deployments: Vec<Arc<Deployment>>,
    payload: Bytes,
    auth: AuthToken,
    domain: String,
) -> Result<(Selection, ResponsePayload), Error> {
    let subgraph_chain = deployments
        .last()
        .map(|deployment| deployment.manifest.network.clone())
        .ok_or_else(|| Error::SubgraphNotFound(anyhow!("no matching deployments")))?;
    tracing::info!(target: reports::CLIENT_QUERY_TARGET, subgraph_chain, domain);
    // Make sure we only select from deployments indexing the same chain. This simplifies dealing
    // with block constraints later.
    deployments.retain(|deployment| deployment.manifest.network == subgraph_chain);
    tracing::info!(deployments = ?deployments.iter().map(|d| d.id).collect::<Vec<_>>());

    let manifest_min_block = deployments.last().unwrap().manifest.min_block;
    let block_cache = *ctx
        .block_caches
        .get(&subgraph_chain)
        .ok_or_else(|| Error::ChainNotFound(subgraph_chain))?;

    match &auth {
        AuthToken::ApiKey(api_key) => tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            user_address = ?api_key.user_address,
            api_key = %api_key.key,
        ),
        AuthToken::Ticket(payload) => tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            user_address = ?payload.user(),
            ticket_payload = serde_json::to_string(payload).unwrap(),
        ),
    };

    let payload: QueryBody =
        serde_json::from_reader(payload.reader()).map_err(|err| Error::BadQuery(err.into()))?;

    ctx.auth_handler
        .check_token(&auth, &deployments, &domain)
        .await
        .map_err(Error::Auth)?;

    let mut indexer_errors: BTreeMap<Address, IndexerError> = Default::default();

    let mut candidates: BTreeSet<Indexing> = deployments
        .iter()
        .flat_map(move |deployment| {
            let id = deployment.id;
            deployment.indexers.iter().map(move |indexer| Indexing {
                indexer: indexer.id,
                deployment: id,
            })
        })
        .collect();
    if candidates.is_empty() {
        return Err(Error::NoIndexers);
    }

    let blocklist = ctx
        .indexings_blocklist
        .value_immediate()
        .unwrap_or_default();
    candidates.retain(|candidate| {
        if blocklist.contains(candidate) {
            indexer_errors.insert(candidate.indexer, IndexerError::Unavailable(NoStatus));
            return false;
        }
        true
    });

    let variables = payload
        .variables
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let mut context = AgoraContext::new(&payload.query, &variables)
        .map_err(|err| Error::BadQuery(anyhow!("{err}")))?;
    tracing::info!(
        target: reports::CLIENT_QUERY_TARGET,
        query = %payload.query,
        %variables,
    );

    let mut candidates: Vec<Candidate> = candidates
        .into_iter()
        .filter_map(|indexing| {
            let cost_model = ctx
                .indexing_statuses
                .value_immediate()
                .and_then(|m| m.get(&indexing).and_then(|s| s.cost_model.clone()));
            let fee = match indexer_fee(&cost_model, &mut context) {
                Ok(fee) => fee,
                Err(err) => {
                    indexer_errors.insert(indexing.indexer, err);
                    return None;
                }
            };
            Some(Candidate { indexing, fee })
        })
        .collect();

    let block_constraints = block_constraints(&context).unwrap_or_default();
    let mut resolved_blocks = join_all(
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

    // Reject queries for blocks before the minimum start block in the manifest, but only if the
    // constraint is for an exact block. For example, we always want to allow `block_gte: 0`.
    let request_contains_invalid_blocks = resolved_blocks
        .iter()
        .filter(|b| {
            block_constraints.iter().any(|c| match c {
                BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => false,
                BlockConstraint::Hash(hash) => hash == &b.hash,
                BlockConstraint::Number(number) => number == &b.number,
            })
        })
        .any(|b| b.number < manifest_min_block);
    if request_contains_invalid_blocks {
        return Err(Error::BadQuery(anyhow!(
            "requested block {}, before minimum `startBlock` of manifest {}",
            min_block.unwrap_or_default(),
            manifest_min_block,
        )));
    }

    let user_settings = ctx.auth_handler.query_settings(&auth).await;

    let grt_per_usd = ctx
        .isa_state
        .latest()
        .network_params
        .grt_per_usd
        .ok_or_else(|| Error::Internal(anyhow!("missing exchange rate")))?;
    // TODO: In the future, we should factor this into our budget somehow.
    let agora_query_count = count_top_level_selection_sets(&context)
        .map_err(Error::BadQuery)?
        .max(1) as u64;
    let candidate_fees: Vec<USD> = candidates
        .iter()
        .map(|c| USD(c.fee.0 / grt_per_usd.0))
        .collect();
    let mut budget = ctx.budgeter.budget(&candidate_fees);
    if let Some(user_budget) = user_settings.budget {
        // Security: Consumers can and will set their budget to unreasonably high values.
        // This `.min` prevents the budget from being set far beyond what it would be
        // automatically. The reason this is important is because sometimes queries are
        // subsidized and we would be at-risk to allow arbitrarily high values.
        let max_budget = USD(budget.0 * UDecimal18::from(10));
        budget = user_budget.min(max_budget);
    }
    let budget = GRT(budget.0 * grt_per_usd.0);
    tracing::info!(
        target: reports::CLIENT_QUERY_TARGET,
        query_count = agora_query_count,
        budget_grt = f64::from(budget.0) as f32,
    );
    candidates.retain(|c| c.fee <= budget);

    let blocks_per_minute = block_cache.blocks_per_minute.value_immediate().unwrap_or(0);
    let mut utility_params = UtilityParameters {
        budget,
        requirements: block_requirements,
        // 170cbcf3-db7f-404a-be13-2022d9142677
        latest_block: 0,
        block_rate_hz: blocks_per_minute as f64 / 60.0,
    };

    let mut rng = SmallRng::from_entropy();
    let mut total_indexer_fees = GRT(UDecimal18::from(0));

    for retry in 0..ctx.indexer_selection_retry_limit {
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

        let selection_timer = METRICS.indexer_selection_duration.start_timer();
        let (mut selections, selection_errors) = ctx
            .isa_state
            .latest()
            .select_indexers(&mut rng, &utility_params, &candidates)
            .map_err(|err| match err {
                InputError::MissingNetworkParams => {
                    Error::Internal(anyhow!("missing network params"))
                }
            })?;
        drop(selection_timer);

        let selections_len = selections.len();
        for (error, indexers) in selection_errors.0 {
            for indexer in indexers {
                let error = match error {
                    SelectionError::NoStatus => IndexerError::Unavailable(NoStatus),
                    SelectionError::NoStake => IndexerError::Unavailable(NoStake),
                    SelectionError::MissingRequiredBlock => IndexerError::Unavailable(MissingBlock),
                    SelectionError::FeeTooHigh => IndexerError::Unavailable(FeeTooHigh),
                    SelectionError::NaN => IndexerError::Internal("NaN"),
                };
                indexer_errors.insert(*indexer, error);
            }
        }
        if selections.is_empty() {
            continue;
        }

        for selection in &mut selections {
            // TODO: In a future where indexers are expected put more effort into setting cost
            // models, we should pay selected indexers the maximum fee of the alternatives
            // (where `fee <= budget`).
            let min_fee = budget.0 * UDecimal18::try_from(0.75 / selections_len as f64).unwrap();
            selection.fee = GRT(selection.fee.0.max(min_fee));
        }
        total_indexer_fees = GRT(total_indexer_fees.0 + selections.iter().map(|s| s.fee.0).sum());
        tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            indexer_fees_grt = f64::from(total_indexer_fees.0) as f32,
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
                kafka_client: ctx.kafka_client,
                attestation_domain: ctx.attestation_domain,
                observations: ctx.observations.clone(),
                deployment,
                response_time: Duration::default(),
            };

            let latest_query_block = pick_latest_query_block(
                block_cache,
                latest_block.number.saturating_sub(selection.blocks_behind),
                blocks_per_minute,
            )
            .await
            .map_err(Error::BlockNotFound)?;

            // The Agora context must be cloned to preserve the state of the original client query.
            // This to avoid the following scenario:
            // 1. A client query has no block requirements set for some top-level operation
            // 2. The first indexer is selected, with some indexing status at block number `n`
            // 3. The query is made deterministic by setting the block requirement to the hash of
            //    block `n`
            // 4. Some condition requires us to retry this query on another indexer with an indexing
            //    status at a block less than `n`
            // 5. The same context is re-used, including the block requirement set to the hash of
            //    block `n`
            // 6. The indexer is seen as being behind and is unnecessarily penalized
            let deterministic_query =
                make_query_deterministic(context.clone(), &resolved_blocks, &latest_query_block)
                    .ok_or_else(|| Error::Internal(anyhow!("failed to set block constraints")))?;

            let optimistic_query = optimistic_query(
                context.clone(),
                &mut resolved_blocks,
                &latest_block,
                &latest_query_block,
                &utility_params.requirements,
                block_cache,
                &selection,
            )
            .await;

            let indexer_query_context = indexer_query_context.clone();
            let outcome_tx = outcome_tx.clone();
            // We must manually construct this span before the spawned task, since otherwise
            // there's a race between creating this span and another indexer responding which will
            // close the outer client_query span.
            let span = tracing::info_span!(
                target: reports::INDEXER_QUERY_TARGET,
                "indexer_query",
                indexer = ?selection.indexing.indexer,
            );
            tokio::spawn(
                async move {
                    let response = handle_indexer_query(
                        indexer_query_context,
                        selection.clone(),
                        deterministic_query,
                        latest_query_block.number,
                        optimistic_query,
                    )
                    .await;
                    let _ = outcome_tx.try_send((selection, response));
                }
                .instrument(span),
            );
        }
        for _ in 0..selections_len {
            match outcome_rx.recv().await {
                None => (),
                Some((selection, Err(err))) => {
                    indexer_errors.insert(selection.indexing.indexer, err);
                }
                Some((selection, Ok(outcome))) => {
                    let total_indexer_fees = USD(total_indexer_fees.0 / grt_per_usd.0);
                    let _ = ctx.budgeter.feedback.send(total_indexer_fees);

                    tracing::debug!(?indexer_errors);

                    return Ok((selection, outcome));
                }
            };
        }
    }

    tracing::debug!(?indexer_errors);
    Err(Error::BadIndexers(IndexerErrors::new(
        indexer_errors.into_values(),
    )))
}

#[derive(Clone)]
struct IndexerQueryContext {
    pub indexer_client: IndexerClient,
    pub kafka_client: &'static KafkaClient,
    pub attestation_domain: &'static Eip712Domain,
    pub observations: QueueWriter<Update>,
    pub deployment: Arc<Deployment>,
    pub response_time: Duration,
}

async fn handle_indexer_query(
    mut ctx: IndexerQueryContext,
    selection: Selection,
    deterministic_query: String,
    latest_query_block: u64,
    optimistic_query: Option<String>,
) -> Result<ResponsePayload, IndexerError> {
    let indexing = selection.indexing;
    let deployment = indexing.deployment.to_string();

    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        %deployment,
        url = %selection.url,
        blocks_behind = selection.blocks_behind,
        fee_grt = f64::from(selection.fee.0) as f32,
        subgraph_chain = %ctx.deployment.manifest.network,
    );

    let optimistic_response = match optimistic_query {
        Some(query) => handle_indexer_query_inner(&mut ctx, selection.clone(), query)
            .await
            .ok(),
        None => None,
    };
    let result = match optimistic_response {
        Some(response) => Ok(response),
        None => handle_indexer_query_inner(&mut ctx, selection, deterministic_query).await,
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
        Err(IndexerError::Unavailable(MissingBlock)) => {
            Err(IndexerErrorObservation::IndexingBehind { latest_query_block })
        }
        Err(_) => Err(IndexerErrorObservation::Other),
    };

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
) -> Result<ResponsePayload, IndexerError> {
    let start_time = Instant::now();
    let result = ctx
        .indexer_client
        .query_indexer(&selection, deterministic_query.clone())
        .await;
    ctx.response_time = Instant::now() - start_time;
    let deployment = selection.indexing.deployment.to_string();
    with_metric(&METRICS.indexer_query.duration, &[&deployment], |hist| {
        hist.observe(ctx.response_time.as_millis() as f64)
    });

    let response = result?;
    if response.status != StatusCode::OK.as_u16() {
        tracing::warn!(indexer_response_status = %response.status);
    }

    let allocation = response.receipt.allocation();
    tracing::info!(target: reports::INDEXER_QUERY_TARGET, ?allocation);

    let indexer_errors = serde_json::from_str::<
        graphql_http::http::response::ResponseBody<Box<RawValue>>,
    >(&response.payload.body)
    .map_err(|err| IndexerError::BadResponse(err.to_string()))?
    .errors
    .into_iter()
    .map(|err| err.message)
    .collect::<Vec<String>>();

    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        indexer_errors = indexer_errors.join(","),
    );

    indexer_errors
        .iter()
        .try_for_each(|err| check_block_error(err))
        .map_err(|_| IndexerError::Unavailable(MissingBlock))?;

    for error in &indexer_errors {
        if miscategorized_unattestable(error) {
            let _ = ctx.observations.write(Update::Penalty {
                indexing: selection.indexing,
            });
            return Err(IndexerError::BadResponse(
                "unattestable response".to_string(),
            ));
        }
    }

    if !ctx.deployment.expect_attestation {
        return Ok(response.payload);
    }

    if response.payload.attestation.is_none() {
        // TODO: This is a temporary hack to handle errors that were previously miscategorized as
        //  unattestable in graph-node.
        for error in &indexer_errors {
            if miscategorized_attestable(error) {
                return Ok(response.payload);
            }
        }

        return Err(IndexerError::BadResponse("no attestation".to_string()));
    }

    if let Some(attestation) = &response.payload.attestation {
        let verified = attestation::verify(
            ctx.attestation_domain,
            attestation,
            &allocation,
            &deterministic_query,
            &response.payload.body,
        );
        // We send the Kafka message directly to avoid passing the request & response payloads
        // through the normal reporting path. This is to reduce log bloat.
        let response = response.payload.body.to_string();
        let payload = serialize_attestation(attestation, allocation, deterministic_query, response);
        ctx.kafka_client.send("gateway_attestations", &payload);
        if let Err(err) = verified {
            return Err(IndexerError::BadResponse(
                anyhow!("bad attestation: {err}").to_string(),
            ));
        }
    }

    Ok(response.payload)
}

fn count_top_level_selection_sets(ctx: &AgoraContext<String>) -> anyhow::Result<usize> {
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

pub fn indexer_fee(
    cost_model: &Option<Ptr<CostModel>>,
    context: &mut AgoraContext<'_, String>,
) -> Result<GRT, IndexerError> {
    match cost_model
        .as_ref()
        .map(|model| model.cost_with_context(context))
    {
        None => Ok(GRT(UDecimal18::from(0))),
        Some(Ok(fee)) => {
            let fee = U256::try_from_be_slice(&fee.to_bytes_be()).unwrap_or(U256::MAX);
            Ok(GRT(UDecimal18::from_raw_u256(fee)))
        }
        Some(Err(_)) => Err(IndexerError::Unavailable(NoFee)),
    }
}

/// Select an available block up to `max_block`. Because the exact block number is not required, we can be a bit more
/// resilient to RPC failures here by backing off on failed block resolution.
async fn pick_latest_query_block(
    cache: &BlockCache,
    max_block: BlockNumber,
    blocks_per_minute: u64,
) -> Result<BlockPointer, UnresolvedBlock> {
    for n in [max_block, max_block - 1, max_block - blocks_per_minute] {
        if let Ok(block) = cache.fetch_block(UnresolvedBlock::WithNumber(n)).await {
            return Ok(block);
        }
    }
    Err(UnresolvedBlock::WithNumber(max_block))
}

/// Create an optimistic query for the indexer at a block closer to chain head than their last
/// reported block. A failed response will not penalize the indexer, and might result in a more
/// recent result for the client. Return `None` if we do not expect the optimistic query to be
/// worth the extra attempt.
async fn optimistic_query(
    ctx: AgoraContext<'_, String>,
    resolved: &mut BTreeSet<BlockPointer>,
    latest: &BlockPointer,
    latest_query_block: &BlockPointer,
    requirements: &BlockRequirements,
    block_cache: &BlockCache,
    selection: &Selection,
) -> Option<String> {
    if !requirements.has_latest {
        return None;
    }
    let blocks_per_minute = block_cache.blocks_per_minute.value_immediate()?;
    if selection.blocks_behind >= blocks_per_minute {
        return None;
    }
    let optimistic_block_number = latest.number.saturating_sub(blocks_per_minute / 30);
    if optimistic_block_number == latest_query_block.number {
        return None;
    }
    let unresolved = UnresolvedBlock::WithNumber(optimistic_block_number);
    let optimistic_block = block_cache.fetch_block(unresolved).await.ok()?;
    resolved.insert(optimistic_block.clone());
    make_query_deterministic(ctx, resolved, &optimistic_block)
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
    use super::*;

    #[tokio::test]
    async fn l2_request_path() {
        let deployment: DeploymentId = "QmdveVMs7nAvdBPxNoaMMAYgNcuSroneMctZDnZUgbPPP3"
            .parse()
            .unwrap();
        let l1_subgraph: SubgraphId = "EMRitnR1t3drKrDQSmJMSmHBPB2sGotgZE12DzWNezDn"
            .parse()
            .unwrap();
        let l2_subgraph: SubgraphId = "CVHoVSrdiiYvLcH4wocDCazJ1YuixHZ1SKt34UWmnQcC"
            .parse()
            .unwrap();

        // test deployment route
        let mut original = format!("/api/deployments/id/{deployment}").parse().unwrap();
        let mut expected = format!("/api/deployments/id/{deployment}");
        assert_eq!(
            expected,
            super::l2_request_path(&original, Some(l2_subgraph))
        );

        // test subgraph route
        original = format!("/api/subgraphs/id/{l1_subgraph}").parse().unwrap();
        expected = format!("/api/subgraphs/id/{l2_subgraph}");
        assert_eq!(
            expected,
            super::l2_request_path(&original, Some(l2_subgraph))
        );

        // test subgraph route with API key prefix
        original = format!("/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{l1_subgraph}")
            .parse()
            .unwrap();
        expected = format!("/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{l2_subgraph}");
        assert_eq!(
            expected,
            super::l2_request_path(&original, Some(l2_subgraph))
        );

        // test subgraph route with version constraint
        original = format!("/api/subgraphs/id/{l1_subgraph}^0.0.1")
            .parse()
            .unwrap();
        expected = format!("/api/subgraphs/id/{l2_subgraph}^0.0.1");
        assert_eq!(
            expected,
            super::l2_request_path(&original, Some(l2_subgraph))
        );
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
