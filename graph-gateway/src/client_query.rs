use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, BlockNumber, U256};
use alloy_sol_types::Eip712Domain;
use anyhow::anyhow;
use axum::extract::OriginalUri;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, Response, StatusCode},
    Extension,
};
use cost_model::{Context as AgoraContext, CostModel};
use eventuals::Ptr;
use futures::future::join_all;
use headers::ContentType;
use prost::bytes::Buf;
use rand::{rngs::SmallRng, SeedableRng as _};
use serde::Deserialize;
use serde_json::value::RawValue;
use thegraph::types::{attestation, BlockPointer, DeploymentId};
use tokio::sync::mpsc;
use toolshed::buffer_queue::QueueWriter;
use tracing::Instrument;

use gateway_common::utils::http_ext::HttpBuilderExt;
use gateway_common::{
    types::{Indexing, UDecimal18, GRT, USD},
    utils::timestamp::unix_timestamp,
};
use gateway_framework::{
    block_constraints::BlockConstraint,
    chains::BlockCache,
    errors::{Error, IndexerError, IndexerErrors, UnavailableReason::*},
    metrics::{with_metric, METRICS},
    scalar::ScalarReceipt,
};
use indexer_selection::{
    actor::Update, BlockRequirements, Candidate, IndexerError as SelectionError,
    IndexerErrorObservation, InputError, Selection, UnresolvedBlock, UtilityParameters,
    SELECTION_LIMIT,
};

use crate::block_constraints::{block_constraints, make_query_deterministic};
use crate::client_query::query_selector::QuerySelector;
use crate::indexer_client::{check_block_error, IndexerClient, ResponsePayload};
use crate::reports::{self, serialize_attestation, KafkaClient};
use crate::topology::{Deployment, GraphNetwork, Subgraph};
use crate::unattestable_errors::{miscategorized_attestable, miscategorized_unattestable};

use self::attestation_header::GraphAttestation;
use self::auth::AuthToken;
use self::context::Context;
use self::l2_forwarding::forward_request_to_l2;

mod attestation_header;
pub mod auth;
pub mod context;
mod l2_forwarding;
pub mod legacy_auth_adapter;
pub mod query_id;
mod query_selector;
pub mod query_tracing;
pub mod require_auth;

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    pub query: String,
    pub variables: Option<Box<RawValue>>,
}

pub async fn handle_query(
    State(ctx): State<Context>,
    Extension(auth): Extension<AuthToken>,
    OriginalUri(original_uri): OriginalUri,
    selector: QuerySelector,
    headers: HeaderMap,
    payload: Bytes,
) -> Result<Response<String>, Error> {
    let start_time = Instant::now();
    let timestamp = unix_timestamp();

    // Check if the query selector is authorized by the auth token
    match &selector {
        QuerySelector::Subgraph(id) => {
            if !auth.is_subgraph_authorized(id) {
                return Err(Error::Auth(anyhow!("Subgraph not authorized by user")));
            }
        }
        QuerySelector::Deployment(id) => {
            if !auth.is_deployment_authorized(id) {
                return Err(Error::Auth(anyhow!("Deployment not authorized by user")));
            }
        }
    }

    let (deployments, subgraph) = resolve_subgraph_deployments(&ctx.network, &selector)?;
    tracing::info!(deployments = ?deployments.iter().map(|d| d.id).collect::<Vec<_>>());

    if let Some(l2_url) = ctx.l2_gateway.as_ref() {
        // Forward query to L2 gateway if it's marked as transferred & there are no allocations.
        // abf62a6d-c071-4507-b528-ddc8e250127a
        let transferred_to_l2 = deployments.iter().all(|d| d.transferred_to_l2);
        if transferred_to_l2 {
            return Ok(forward_request_to_l2(
                &ctx.indexer_client.client,
                l2_url,
                &original_uri,
                headers,
                payload,
                subgraph.and_then(|s| s.l2_id),
            )
            .await);
        }
    }

    let result = handle_client_query_inner(&ctx, deployments, payload, auth)
        .in_current_span()
        .await;

    // Metrics and tracing
    {
        let deployment: Option<String> = result
            .as_ref()
            .map(|(selection, _)| selection.indexing.deployment.to_string())
            .ok();
        let metric_labels = [deployment.as_deref().unwrap_or("")];

        METRICS.client_query.check(&metric_labels, &result);
        with_metric(&METRICS.client_query.duration, &metric_labels, |h| {
            h.observe((Instant::now() - start_time).as_secs_f64())
        });

        let status_message = match &result {
            Ok(_) => "200 OK".to_string(),
            Err(err) => err.to_string(),
        };
        let (legacy_status_message, legacy_status_code) = reports::legacy_status(&result);
        tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            start_time_ms = timestamp,
            deployment,
            %status_message,
            %legacy_status_message,
            legacy_status_code,
        );
    }

    result.map(|(_, ResponsePayload { body, attestation })| {
        Response::builder()
            .status(StatusCode::OK)
            .header_typed(ContentType::json())
            .header_typed(GraphAttestation(attestation))
            .body(body.to_string())
            .unwrap()
    })
}

/// Given a query selector, resolve the subgraph deployments for the query. If the selector is a subgraph ID, return
/// the subgraph's deployment instances. If the selector is a deployment ID, return the deployment instance.
fn resolve_subgraph_deployments(
    network: &GraphNetwork,
    selector: &QuerySelector,
) -> Result<(Vec<Arc<Deployment>>, Option<Subgraph>), Error> {
    match selector {
        QuerySelector::Subgraph(subgraph_id) => {
            // Get the subgraph by ID
            let subgraph = network
                .subgraph_by_id(subgraph_id)
                .ok_or_else(|| Error::SubgraphNotFound(anyhow!("{subgraph_id}")))?;

            // Get the subgraph's chain (from the last of its deployments)
            let subgraph_chain = subgraph
                .deployments
                .last()
                .map(|deployment| deployment.manifest.network.clone())
                .ok_or_else(|| Error::SubgraphNotFound(anyhow!("no matching deployments")))?;

            // Get the subgraph's deployments. Make sure we only select from deployments indexing
            // the same chain. This simplifies dealing with block constraints later
            let versions = subgraph
                .deployments
                .iter()
                .filter(|deployment| deployment.manifest.network == subgraph_chain)
                .cloned()
                .collect();

            Ok((versions, Some(subgraph)))
        }
        QuerySelector::Deployment(deployment_id) => {
            // Get the deployment by ID
            let deployment = network.deployment_by_id(deployment_id).ok_or_else(|| {
                Error::SubgraphNotFound(anyhow!("deployment not found: {deployment_id}"))
            })?;

            Ok((vec![deployment], None))
        }
    }
}

async fn handle_client_query_inner(
    ctx: &Context,
    deployments: Vec<Arc<Deployment>>,
    payload: Bytes,
    auth: AuthToken,
) -> Result<(Selection, ResponsePayload), Error> {
    let subgraph_chain = deployments
        .last()
        .map(|deployment| deployment.manifest.network.clone())
        .ok_or_else(|| Error::SubgraphNotFound(anyhow!("no matching deployments")))?;
    tracing::info!(target: reports::CLIENT_QUERY_TARGET, subgraph_chain);

    let manifest_min_block = deployments.last().unwrap().manifest.min_block;
    let block_cache = *ctx
        .block_caches
        .get(&subgraph_chain)
        .ok_or_else(|| Error::ChainNotFound(subgraph_chain))?;

    let payload: QueryBody =
        serde_json::from_reader(payload.reader()).map_err(|err| Error::BadQuery(err.into()))?;

    ctx.auth_handler
        .check_token(&auth, &deployments)
        .await
        .map_err(Error::Auth)?;

    let mut indexer_errors: BTreeMap<Address, IndexerError> = Default::default();

    let mut candidates: BTreeSet<Indexing> = deployments
        .iter()
        .flat_map(move |deployment| {
            let id = deployment.id;
            deployment.indexers.keys().map(move |indexer| Indexing {
                indexer: *indexer,
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
        if blocklist.contains(candidate) || ctx.bad_indexers.contains(&candidate.indexer) {
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

    let grt_per_usd = ctx
        .isa_state
        .latest()
        .network_params
        .grt_per_usd
        .ok_or_else(|| Error::Internal(anyhow!("missing exchange rate")))?;
    let mut budget = GRT(ctx.budgeter.query_fees_target.0 * grt_per_usd.0);
    let query_settings = auth.query_settings();
    if let Some(user_budget) = query_settings.budget {
        // Security: Consumers can and will set their budget to unreasonably high values.
        // This `.min` prevents the budget from being set far beyond what it would be
        // automatically. The reason this is important is that sometimes queries are
        // subsidized, and we would be at-risk to allow arbitrarily high values.
        let max_budget = GRT(budget.0 * UDecimal18::from(10));
        budget = GRT(user_budget.0 * grt_per_usd.0).min(max_budget);
    }
    tracing::info!(
        target: reports::CLIENT_QUERY_TARGET,
        query_count = 1,
        budget_grt = f64::from(budget.0) as f32,
    );

    let versions_behind: BTreeMap<DeploymentId, u8> = deployments
        .iter()
        .rev()
        .enumerate()
        .map(|(index, deployment)| (deployment.id, index.try_into().unwrap_or(u8::MAX)))
        .collect();

    let candidates: Vec<Candidate> = candidates
        .into_iter()
        .filter_map(|indexing| {
            let cost_model = ctx
                .indexing_statuses
                .value_immediate()
                .and_then(|m| m.get(&indexing).and_then(|s| s.cost_model.clone()));
            let fee = match indexer_fee(&cost_model, &mut context) {
                // Clamp indexer fee to the budget.
                Ok(fee) => fee.min(budget),
                Err(err) => {
                    indexer_errors.insert(indexing.indexer, err);
                    return None;
                }
            };
            Some(Candidate {
                indexing,
                fee,
                versions_behind: *versions_behind.get(&indexing.deployment).unwrap_or(&0),
            })
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
                    SelectionError::FeeTooHigh => IndexerError::Internal("fee too high"),
                    SelectionError::NaN => IndexerError::Internal("NaN"),
                };
                indexer_errors.insert(*indexer, error);
            }
        }
        if selections.is_empty() {
            continue;
        }

        for selection in &mut selections {
            let min_fee = ctx
                .budgeter
                .min_indexer_fees
                .value_immediate()
                .unwrap_or_default();
            let min_fee =
                GRT((min_fee.0 * grt_per_usd.0) / UDecimal18::from(selections_len as u128));
            selection.fee = selection.fee.max(min_fee);
        }

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
                make_query_deterministic(context.clone(), &resolved_blocks, &latest_query_block)?;

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

            // When we prepare an optimistic query, we are pessimistically doubling the fee in case
            // the optimistic query fails and we pay again for the fallback query.
            let selection_fee = optimistic_query
                .is_some()
                .then(|| GRT(selection.fee.0 * 2.into()))
                .unwrap_or(selection.fee);
            total_indexer_fees = GRT(total_indexer_fees.0 + selection_fee.0);

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

        tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            indexer_fees_grt = f64::from(total_indexer_fees.0) as f32,
        );

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
    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        ?allocation,
        legacy_scalar = matches!(&response.receipt, ScalarReceipt::Legacy(_)),
    );

    let indexer_errors = serde_json::from_str::<
        graphql_http::http::response::ResponseBody<Box<RawValue>>,
    >(&response.payload.body)
    .map_err(|err| IndexerError::BadResponse(err.to_string()))?
    .errors
    .into_iter()
    .map(|mut err| {
        err.message.truncate(512);
        err.message.shrink_to_fit();
        err.message
    })
    .collect::<Vec<String>>();

    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        indexer_errors = indexer_errors.join("; "),
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
            let message = if !indexer_errors.is_empty() {
                format!("unattestable response: {}", indexer_errors.join("; "))
            } else {
                "unattestable response".to_string()
            };
            return Err(IndexerError::BadResponse(message));
        }
    }

    if response.payload.attestation.is_none() {
        // TODO: This is a temporary hack to handle errors that were previously miscategorized as
        //  unattestable in graph-node.
        for error in &indexer_errors {
            if miscategorized_attestable(error) {
                return Ok(response.payload);
            }
        }

        let message = if !indexer_errors.is_empty() {
            format!("no attestation: {}", indexer_errors.join("; "))
        } else {
            "no attestation".to_string()
        };
        return Err(IndexerError::BadResponse(message));
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
    let min_block = requirements.range.map(|(min, _)| min).unwrap_or(0);
    let optimistic_block_number = latest
        .number
        .saturating_sub(blocks_per_minute / 30)
        .max(min_block);
    if optimistic_block_number <= latest_query_block.number {
        return None;
    }
    let unresolved = UnresolvedBlock::WithNumber(optimistic_block_number);
    let optimistic_block = block_cache.fetch_block(unresolved).await.ok()?;
    resolved.insert(optimistic_block.clone());
    make_query_deterministic(ctx, resolved, &optimistic_block).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    mod require_req_auth {
        use std::collections::HashMap;
        use std::sync::Arc;

        use assert_matches::assert_matches;
        use axum::body::BoxBody;
        use axum::http::{Method, Request, StatusCode};
        use axum::routing::post;
        use axum::{middleware, Extension, Router};
        use eventuals::{Eventual, Ptr};
        use headers::{Authorization, ContentType, HeaderMapExt};
        use hyper::Body;
        use tower::ServiceExt;

        use crate::subgraph_studio::APIKey;

        use super::auth::{AuthContext, AuthToken};
        use super::legacy_auth_adapter::legacy_auth_adapter;
        use super::require_auth::RequireAuthorizationLayer;

        /// Create a test authorization context.
        fn test_auth_ctx(key: Option<&str>) -> AuthContext {
            let mut ctx = AuthContext {
                api_keys: Eventual::from_value(Ptr::new(Default::default())),
                special_api_keys: Default::default(),
                special_query_key_signers: Default::default(),
                api_key_payment_required: false,
                subscriptions: Eventual::from_value(Ptr::new(Default::default())),
                subscription_rate_per_query: 0,
                subscription_domains: Default::default(),
                subscription_query_counters: Default::default(),
            };
            if let Some(key) = key {
                ctx.api_keys = Eventual::from_value(Ptr::new(HashMap::from([(
                    key.into(),
                    Arc::new(APIKey {
                        key: key.into(),
                        ..Default::default()
                    }),
                )])));
            }
            ctx
        }

        /// Create a test request without an `Authorization` header or `AuthToken` extension.
        fn test_req_unauthenticated() -> Request<Body> {
            Request::builder()
                .method(Method::POST)
                .uri("/subgraphs/id/123")
                .body(Body::empty())
                .unwrap()
        }

        /// Create a test request with an `Authorization` header.
        fn test_req_with_auth_header(token: &str) -> Request<Body> {
            let mut req = Request::builder()
                .method(Method::POST)
                .uri("/subgraphs/id/123")
                .body(Body::empty())
                .unwrap();

            let bearer_token = Authorization::bearer(token).expect("valid bearer token");
            req.headers_mut().typed_insert(bearer_token);

            req
        }

        /// Create a test request with legacy authorization-in-path scheme.
        fn test_req_with_legacy_auth(token: &str) -> Request<Body> {
            Request::builder()
                .method(Method::POST)
                .uri(format!("/{token}/subgraphs/id/123"))
                .body(Body::empty())
                .unwrap()
        }

        /// Create a test router that requires authorization and also supports legacy authorization-in-path
        /// scheme.
        fn test_router(auth_ctx: AuthContext) -> Router {
            async fn handler(Extension(auth): Extension<AuthToken>) -> String {
                match auth {
                    AuthToken::StudioApiKey(api_key) => api_key.key.clone(),
                    _ => unreachable!(),
                }
            }

            Router::new()
                .route("/subgraphs/id/:subgraph_id", post(handler))
                .route("/:api_key/subgraphs/id/:subgraph_id", post(handler))
                .layer(
                    tower::ServiceBuilder::new()
                        .layer(middleware::from_fn(legacy_auth_adapter))
                        .layer(RequireAuthorizationLayer::new(auth_ctx)),
                )
        }

        /// Deserialize a GraphQL response body.
        async fn deserialize_graphql_response_body<T>(
            body: &mut BoxBody,
        ) -> serde_json::Result<graphql_http::http::response::ResponseBody<T>>
        where
            for<'de> T: serde::Deserialize<'de>,
        {
            let body = hyper::body::to_bytes(body).await.expect("valid body");
            serde_json::from_slice(body.as_ref())
        }

        /// Parse text response body.
        async fn parse_text_response_body(body: &mut BoxBody) -> anyhow::Result<String> {
            let body = hyper::body::to_bytes(body).await.expect("valid body");
            let text = String::from_utf8(body.to_vec())?;
            Ok(text)
        }

        #[tokio::test]
        async fn reject_non_authorized_request() {
            //* Given
            let app = test_router(test_auth_ctx(None));

            let req = test_req_unauthenticated();

            //* When
            let mut res = app.oneshot(req).await.expect("to be infallible");

            //* Then
            assert_eq!(res.status(), StatusCode::OK);
            assert_eq!(
                res.headers().typed_get::<ContentType>(),
                Some(ContentType::json())
            );
            assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body.errors.len(), 1);
                assert_eq!(res_body.errors[0].message, "auth error: Missing Authorization header");
            });
        }

        #[tokio::test]
        async fn reject_authorized_request_with_invalid_api_key() {
            //* Given
            let api_key = "0123456789abcdef0123456789abcdef";

            // We do not insert the API key into the auth context, so it will be rejected
            let app = test_router(test_auth_ctx(None));

            let req = test_req_with_auth_header(api_key);

            //* When
            let mut res = app.oneshot(req).await.expect("to be infallible");

            //* Then
            assert_eq!(res.status(), StatusCode::OK);
            assert_eq!(
                res.headers().typed_get::<ContentType>(),
                Some(ContentType::json())
            );
            assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body.errors.len(), 1);
                assert_eq!(res_body.errors[0].message, "auth error: Invalid bearer token: Invalid auth token");
            });
        }

        #[tokio::test]
        async fn accept_authorized_request() {
            //* Given
            let api_key = "0123456789abcdef0123456789abcdef";

            let app = test_router(test_auth_ctx(Some(api_key)));

            let req = test_req_with_auth_header(api_key);

            //* When
            let mut res = app.oneshot(req).await.expect("to be infallible");

            //* Then
            assert_eq!(res.status(), StatusCode::OK);
            assert_matches!(parse_text_response_body(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body, api_key);
            });
        }

        #[tokio::test]
        async fn accept_authorized_request_with_legacy_scheme() {
            //* Given
            let api_key = "0123456789abcdef0123456789abcdef";

            let app = test_router(test_auth_ctx(Some(api_key)));

            let req = test_req_with_legacy_auth(api_key);

            //* When
            let mut res = app.oneshot(req).await.expect("to be infallible");

            //* Then
            assert_eq!(res.status(), StatusCode::OK);
            assert_matches!(parse_text_response_body(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body, api_key);
            });
        }
    }
}
