use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, BlockNumber};
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
use gateway_framework::budgets::USD;
use gateway_framework::chains::UnresolvedBlock;
use gateway_framework::errors::UnavailableReason;
use gateway_framework::scalar::ReceiptStatus;
use headers::ContentType;
use indexer_selection::{ArrayVec, Candidate, ExpectedPerformance, Normalized, Performance};
use num_traits::cast::ToPrimitive as _;
use ordered_float::NotNan;
use prost::bytes::Buf;
use rand::Rng as _;
use rand::{rngs::SmallRng, SeedableRng as _};
use serde::Deserialize;
use serde_json::value::RawValue;
use thegraph::types::{attestation, BlockPointer, DeploymentId};
use tokio::sync::mpsc;
use toolshed::url::Url;
use tracing::Instrument;

use gateway_common::utils::http_ext::HttpBuilderExt;
use gateway_common::{types::Indexing, utils::timestamp::unix_timestamp};
use gateway_framework::{
    block_constraints::BlockConstraint,
    chains::BlockCache,
    errors::{Error, IndexerError, IndexerErrors, UnavailableReason::*},
    metrics::{with_metric, METRICS},
    scalar::ScalarReceipt,
};

use crate::block_constraints::{block_constraints, make_query_deterministic};
use crate::indexer_client::{check_block_error, IndexerClient, ResponsePayload};
use crate::indexers::indexing;
use crate::indexing_performance::IndexingPerformance;
use crate::reports::{self, serialize_attestation, KafkaClient};
use crate::topology::{Deployment, GraphNetwork, Subgraph};
use crate::unattestable_errors::{miscategorized_attestable, miscategorized_unattestable};

use self::attestation_header::GraphAttestation;
use self::auth::AuthToken;
use self::context::Context;
use self::l2_forwarding::forward_request_to_l2;
use self::query_selector::QuerySelector;
use self::query_settings::QuerySettings;

mod attestation_header;
pub mod auth;
pub mod context;
mod l2_forwarding;
pub mod legacy_auth_adapter;
pub mod query_id;
mod query_selector;
mod query_settings;
pub mod query_tracing;
pub mod rate_limiter;
pub mod require_auth;

const SELECTION_LIMIT: usize = 3;

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    pub query: String,
    pub variables: Option<Box<RawValue>>,
}

pub struct Selection {
    pub indexing: Indexing,
    pub url: Url,
    pub receipt: ScalarReceipt,
    pub blocks_behind: u64,
}

#[derive(Debug)]
struct BlockRequirements {
    /// required block range, for exact block constraints (`number` & `hash`)
    range: Option<(BlockNumber, BlockNumber)>,
    /// maximum `number_gte` constraint
    number_gte: Option<BlockNumber>,
    /// does the query benefit from using the latest block (contains NumberGTE or Unconstrained)
    latest: bool,
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_query(
    State(ctx): State<Context>,
    Extension(auth): Extension<AuthToken>,
    query_settings: Option<Extension<QuerySettings>>,
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

    // Check authorization for the resolved deployments
    if !auth.are_deployments_authorized(&deployments) {
        return Err(Error::Auth(anyhow::anyhow!(
            "deployment not authorized by user"
        )));
    }

    if !auth.are_subgraphs_authorized(&deployments) {
        return Err(Error::Auth(anyhow::anyhow!(
            "subgraph not authorized by user"
        )));
    }

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

    let result = handle_client_query_inner(
        &ctx,
        query_settings.map(|Extension(settings)| settings),
        deployments,
        payload,
    )
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
    query_settings: Option<QuerySettings>,
    deployments: Vec<Arc<Deployment>>,
    payload: Bytes,
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

    let mut indexer_errors: BTreeMap<Address, IndexerError> = Default::default();

    let mut available_indexers: BTreeSet<Indexing> = deployments
        .iter()
        .flat_map(move |deployment| {
            let id = deployment.id;
            deployment.indexers.keys().map(move |indexer| Indexing {
                indexer: *indexer,
                deployment: id,
            })
        })
        .collect();
    let blocklist = ctx
        .indexings_blocklist
        .value_immediate()
        .unwrap_or_default();
    available_indexers.retain(|candidate| {
        if blocklist.contains(candidate) || ctx.bad_indexers.contains(&candidate.indexer) {
            indexer_errors.insert(candidate.indexer, IndexerError::Unavailable(NoStatus));
            return false;
        }
        true
    });
    if available_indexers.is_empty() {
        return Err(Error::NoIndexers);
    }

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
        .grt_per_usd
        .value_immediate()
        .ok_or_else(|| Error::Internal(anyhow!("missing exchange rate")))?;
    let one_grt = NotNan::new(1e18).unwrap();
    let mut budget = *(ctx.budgeter.query_fees_target.0 * grt_per_usd * one_grt) as u128;
    let query_settings = query_settings.unwrap_or_default();
    if let Some(user_budget_usd) = query_settings.budget_usd {
        // Security: Consumers can and will set their budget to unreasonably high values.
        // This `.min` prevents the budget from being set far beyond what it would be
        // automatically. The reason this is important is that sometimes queries are
        // subsidized, and we would be at-risk to allow arbitrarily high values.
        let max_budget = budget * 10;
        budget = (*(user_budget_usd * grt_per_usd * one_grt) as u128).min(max_budget);
    }
    tracing::info!(
        target: reports::CLIENT_QUERY_TARGET,
        query_count = 1,
        budget_grt = (budget as f64 * 1e-18) as f32,
    );

    let versions_behind: BTreeMap<DeploymentId, u8> = deployments
        .iter()
        .rev()
        .enumerate()
        .map(|(index, deployment)| (deployment.id, index.try_into().unwrap_or(u8::MAX)))
        .collect();

    let chain_head = block_cache
        .chain_head
        .value_immediate()
        .ok_or(Error::BlockNotFound(UnresolvedBlock::WithNumber(0)))?
        .number;
    let blocks_per_minute = block_cache.blocks_per_minute.value_immediate().unwrap_or(0);
    tracing::debug!(chain_head, blocks_per_minute);

    let mut resolved_blocks = Default::default();
    let block_requirements = resolve_block_requirements(
        block_cache,
        &mut resolved_blocks,
        &context,
        manifest_min_block,
        chain_head,
    )
    .await?;

    let indexing_statuses = ctx.indexing_statuses.value_immediate().unwrap();
    let mut candidates = Vec::new();
    {
        let perf = ctx.indexing_perf.latest();
        for indexing in available_indexers {
            match prepare_candidate(
                &ctx.network,
                &indexing_statuses,
                &perf,
                &versions_behind,
                &mut context,
                &block_requirements,
                chain_head,
                blocks_per_minute,
                grt_per_usd,
                budget,
                indexing,
            ) {
                Ok(candidate) => candidates.push(candidate),
                Err(indexer_error) => {
                    indexer_errors.insert(indexing.indexer, indexer_error);
                }
            }
        }
    }

    let mut rng = SmallRng::from_entropy();
    if tracing::enabled!(tracing::Level::TRACE) {
        tracing::trace!(?candidates);
    } else if rng.gen_bool(0.001) {
        tracing::debug!(?candidates);
    }

    if candidates.is_empty() {
        tracing::debug!(?indexer_errors);
        return Err(Error::BadIndexers(IndexerErrors::new(
            indexer_errors.into_values(),
        )));
    }

    let blocks_per_minute = block_cache.blocks_per_minute.value_immediate().unwrap_or(0);

    let mut total_indexer_fees_grt: u128 = 0;
    for retry in 0..ctx.indexer_selection_retry_limit {
        // Make sure our observations are up-to-date if retrying.
        if retry > 0 {
            ctx.indexing_perf.flush().await;

            // Update candidate performance.
            let perf = ctx.indexing_perf.latest();
            for candidate in &mut candidates {
                if let Some(updated) = perf.get(&Indexing {
                    indexer: candidate.indexer,
                    deployment: candidate.deployment,
                }) {
                    candidate.perf = updated.expected_performance();
                }
            }
        }

        let selected_candidates: ArrayVec<&Candidate, SELECTION_LIMIT> =
            indexer_selection::select(&mut rng, &candidates);
        let selections_len = selected_candidates.len();
        let mut selections: Vec<Selection> = Default::default();
        for candidate in selected_candidates {
            let indexing = Indexing {
                indexer: candidate.indexer,
                deployment: candidate.deployment,
            };

            // over-pay indexers to hit target
            let min_fee = ctx
                .budgeter
                .min_indexer_fees
                .value_immediate()
                .unwrap_or_default();
            let min_fee = *(min_fee.0 * grt_per_usd * one_grt) / selections_len as f64;
            let indexer_fee = candidate.fee.as_f64() * budget as f64;
            let fee = indexer_fee.max(min_fee) as u128;

            let receipt = match ctx.receipt_signer.create_receipt(&indexing, fee).await {
                Some(receipt) => receipt,
                None => {
                    tracing::error!(?indexing, "failed to create receipt");
                    continue;
                }
            };
            debug_assert!(fee == receipt.grt_value());

            let blocks_behind = (candidate.seconds_behind as f64 / 60.0) * blocks_per_minute as f64;
            selections.push(Selection {
                indexing,
                url: candidate.url.clone(),
                receipt,
                blocks_behind: blocks_behind as u64,
            });
        }
        let selections_len = selections.len();
        if selections.is_empty() {
            // Candidates that would never be selected should be filtered out for improved errors.
            tracing::error!("no candidates selected");
            continue;
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
                indexing_perf: ctx.indexing_perf.clone(),
                deployment,
                response_time: Duration::default(),
            };

            let latest_query_block = pick_latest_query_block(
                block_cache,
                block_requirements.number_gte,
                chain_head.saturating_sub(selection.blocks_behind),
                blocks_per_minute,
            )
            .await
            .map_err(Error::BlockNotFound)?;
            tracing::debug!(
                indexer = %selection.indexing.indexer,
                latest_query_block = latest_query_block.number,
            );

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
                chain_head,
                &latest_query_block,
                &block_requirements,
                block_cache,
                &selection,
            )
            .await;

            total_indexer_fees_grt += selection.receipt.grt_value();

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
            let receipt_signer = ctx.receipt_signer;
            tokio::spawn(
                async move {
                    let response = handle_indexer_query(
                        indexer_query_context,
                        &selection,
                        deterministic_query,
                        optimistic_query,
                    )
                    .await;
                    let receipt_status = match &response {
                        Ok(_) => ReceiptStatus::Success,
                        Err(IndexerError::Timeout) => ReceiptStatus::Unknown,
                        Err(_) => ReceiptStatus::Failure,
                    };
                    receipt_signer
                        .record_receipt(&selection.indexing, &selection.receipt, receipt_status)
                        .await;

                    let _ = outcome_tx.try_send((selection, response));
                }
                .instrument(span),
            );
        }

        let total_indexer_fees_usd =
            USD(NotNan::new(total_indexer_fees_grt as f64 * 1e-18).unwrap() / grt_per_usd);
        tracing::info!(
            target: reports::CLIENT_QUERY_TARGET,
            indexer_fees_grt = (total_indexer_fees_grt as f64 * 1e-18) as f32,
            indexer_fees_usd = *total_indexer_fees_usd.0 as f32,
        );

        for _ in 0..selections_len {
            match outcome_rx.recv().await {
                None => (),
                Some((selection, Err(err))) => {
                    indexer_errors.insert(selection.indexing.indexer, err);
                }
                Some((selection, Ok(outcome))) => {
                    let _ = ctx.budgeter.feedback.send(total_indexer_fees_usd);

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

async fn resolve_block_requirements(
    block_cache: &BlockCache,
    resolved_blocks: &mut BTreeSet<BlockPointer>,
    context: &AgoraContext<'_, String>,
    manifest_min_block: BlockNumber,
    chain_head: BlockNumber,
) -> Result<BlockRequirements, Error> {
    let constraints = block_constraints(context).unwrap_or_default();

    let latest = constraints.iter().any(|c| match c {
        BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => true,
        BlockConstraint::Hash(_) | BlockConstraint::Number(_) => false,
    });
    let number_gte = constraints
        .iter()
        .filter_map(|c| match c {
            BlockConstraint::NumberGTE(n) => Some(*n),
            _ => None,
        })
        .max();

    resolved_blocks.append(
        &mut join_all(
            constraints
                .iter()
                .filter(|c| match c {
                    BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => false,
                    BlockConstraint::Number(number) => *number >= (chain_head.saturating_sub(500)),
                    BlockConstraint::Hash(_) => true,
                })
                .filter_map(|constraint| constraint.clone().into_unresolved())
                .map(|unresolved| block_cache.fetch_block(unresolved)),
        )
        .await
        .into_iter()
        .collect::<Result<BTreeSet<BlockPointer>, UnresolvedBlock>>()
        .map_err(Error::BlockNotFound)?,
    );
    let exact_constraints: Vec<u64> = constraints
        .iter()
        .filter_map(|c| match c {
            BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => None,
            BlockConstraint::Number(number) => Some(*number),
            BlockConstraint::Hash(hash) => resolved_blocks
                .iter()
                .find(|b| hash == &b.hash)
                .map(|b| b.number),
        })
        .collect();
    let min_block = exact_constraints.iter().min().cloned();
    let max_block = exact_constraints.iter().max().cloned();

    // Reject queries for blocks before the minimum start block in the manifest, but only if the
    // constraint is for an exact block. For example, we always want to allow `block_gte: 0`.
    let request_contains_invalid_blocks = exact_constraints
        .iter()
        .any(|number| *number < manifest_min_block);
    if request_contains_invalid_blocks {
        return Err(Error::BadQuery(anyhow!(
            "requested block {}, before minimum `startBlock` of manifest {}",
            min_block.unwrap_or_default(),
            manifest_min_block,
        )));
    }

    Ok(BlockRequirements {
        range: min_block.map(|min| (min, max_block.unwrap())),
        number_gte,
        latest,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_candidate(
    network: &GraphNetwork,
    statuses: &HashMap<Indexing, indexing::Status>,
    perf: &HashMap<Indexing, Performance>,
    versions_behind: &BTreeMap<DeploymentId, u8>,
    context: &mut AgoraContext<String>,
    block_requirements: &BlockRequirements,
    chain_head: BlockNumber,
    blocks_per_minute: u64,
    grt_per_usd: NotNan<f64>,
    budget: u128,
    indexing: Indexing,
) -> Result<Candidate, IndexerError> {
    let info = network
        .indexing(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let status = statuses
        .get(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let perf = perf
        .get(&indexing)
        .map(|p| p.expected_performance())
        .unwrap_or(ExpectedPerformance {
            success_rate: Normalized::ONE,
            latency_success_ms: 0,
            latency_failure_ms: 0,
        });

    let fee = Normalized::new(indexer_fee(&status.cost_model, context)? as f64 / budget as f64)
        .unwrap_or(Normalized::ONE);

    if let Some((min, max)) = &block_requirements.range {
        // Allow indexers if their last reported block is "close enough" to the required block
        // range. This is to compensate for the gateway's lack of knowledge about which blocks
        // indexers have responded with already. All else being equal, indexers closer to chain head
        // and with higher success rate will be favored.
        let range = status.min_block.unwrap_or(0)..=(status.block + blocks_per_minute);
        let number_gte = block_requirements.number_gte.unwrap_or(0);
        if !range.contains(min) || !range.contains(max) || (*range.end() < number_gte) {
            return Err(IndexerError::Unavailable(UnavailableReason::MissingBlock));
        }
    }

    let seconds_behind = if !block_requirements.latest || (blocks_per_minute == 0) {
        0
    } else {
        ((chain_head.saturating_sub(status.block) as f64 / blocks_per_minute as f64) * 60.0).ceil()
            as u32
    };

    let slashable_usd = ((info.staked_tokens as f64 * 1e-18) / *grt_per_usd) as u64;
    if slashable_usd == 0 {
        return Err(IndexerError::Unavailable(NoStake));
    }

    Ok(Candidate {
        indexer: indexing.indexer,
        deployment: indexing.deployment,
        url: info.url.clone(),
        perf,
        fee,
        seconds_behind,
        slashable_usd,
        subgraph_versions_behind: *versions_behind.get(&indexing.deployment).unwrap_or(&0),
        zero_allocation: info.allocated_tokens == 0,
    })
}

#[derive(Clone)]
struct IndexerQueryContext {
    pub indexer_client: IndexerClient,
    pub kafka_client: &'static KafkaClient,
    pub attestation_domain: &'static Eip712Domain,
    pub indexing_perf: IndexingPerformance,
    pub deployment: Arc<Deployment>,
    pub response_time: Duration,
}

async fn handle_indexer_query(
    mut ctx: IndexerQueryContext,
    selection: &Selection,
    deterministic_query: String,
    optimistic_query: Option<String>,
) -> Result<ResponsePayload, IndexerError> {
    let indexing = selection.indexing;
    let deployment = indexing.deployment.to_string();

    let optimistic_response = match optimistic_query {
        Some(query) => handle_indexer_query_inner(&mut ctx, selection, query)
            .await
            .ok(),
        None => None,
    };
    let result = match optimistic_response {
        Some(response) => Ok(response),
        None => handle_indexer_query_inner(&mut ctx, selection, deterministic_query).await,
    };
    METRICS.indexer_query.check(&[&deployment], &result);

    let latency_ms = ctx.response_time.as_millis() as u32;
    tracing::info!(
        target: reports::INDEXER_QUERY_TARGET,
        %deployment,
        url = %selection.url,
        blocks_behind = selection.blocks_behind,
        fee_grt = (selection.receipt.grt_value() as f64 * 1e-18) as f32,
        allocation = ?selection.receipt.allocation(),
        legacy_scalar = matches!(&selection.receipt, ScalarReceipt::Legacy(_, _)),
        subgraph_chain = %ctx.deployment.manifest.network,
        response_time_ms = latency_ms,
        status_message = match &result {
            Ok(_) => "200 OK".to_string(),
            Err(err) => format!("{err:?}"),
        },
        status_code = reports::indexer_attempt_status_code(&result),
    );

    ctx.indexing_perf
        .feedback(indexing, result.is_ok(), latency_ms);

    result
}

async fn handle_indexer_query_inner(
    ctx: &mut IndexerQueryContext,
    selection: &Selection,
    deterministic_query: String,
) -> Result<ResponsePayload, IndexerError> {
    let start_time = Instant::now();
    let result = ctx
        .indexer_client
        .query_indexer(selection, deterministic_query.clone())
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
        let allocation = selection.receipt.allocation();
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
) -> Result<u128, IndexerError> {
    match cost_model
        .as_ref()
        .map(|model| model.cost_with_context(context))
    {
        None => Ok(0),
        Some(Ok(fee)) => fee.to_u128().ok_or(IndexerError::Unavailable(NoFee)),
        Some(Err(_)) => Err(IndexerError::Unavailable(NoFee)),
    }
}

/// Select an available block up to `max_block`. Because the exact block number is not required, we can be a bit more
/// resilient to RPC failures here by backing off on failed block resolution.
async fn pick_latest_query_block(
    cache: &BlockCache,
    min_block: Option<BlockNumber>,
    max_block: BlockNumber,
    blocks_per_minute: u64,
) -> Result<BlockPointer, UnresolvedBlock> {
    for mut n in [
        max_block,
        max_block.saturating_sub(1),
        max_block.saturating_sub(blocks_per_minute),
    ] {
        n = n.max(min_block.unwrap_or(0));
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
    chain_head: BlockNumber,
    latest_query_block: &BlockPointer,
    requirements: &BlockRequirements,
    block_cache: &BlockCache,
    selection: &Selection,
) -> Option<String> {
    if !requirements.latest {
        return None;
    }
    let blocks_per_minute = block_cache.blocks_per_minute.value_immediate()?;
    if selection.blocks_behind >= blocks_per_minute {
        return None;
    }
    let min_block = requirements.range.map(|(min, _)| min).unwrap_or(0);
    let optimistic_block_number = chain_head
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
                payment_required: false,
                api_keys: Eventual::from_value(Ptr::new(Default::default())),
                special_api_keys: Default::default(),
                special_query_key_signers: Default::default(),
                subscriptions: Eventual::from_value(Ptr::new(Default::default())),
                subscription_rate_per_query: 0,
                subscription_domains: Default::default(),
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
                    AuthToken::StudioApiKey(auth_token) => auth_token.key().to_string(),
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
                assert_eq!(res_body.errors[0].message, "auth error: missing authorization header");
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
                assert_eq!(res_body.errors[0].message, "auth error: invalid bearer token: invalid auth token");
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
