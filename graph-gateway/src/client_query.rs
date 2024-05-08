use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, BlockHash, BlockNumber};
use alloy_sol_types::Eip712Domain;
use anyhow::anyhow;
use axum::{
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Response, StatusCode},
    Extension,
};
use cost_model::{Context as AgoraContext, CostModel};
use eventuals::Ptr;
use gateway_common::{
    types::Indexing,
    utils::{http_ext::HttpBuilderExt, timestamp::unix_timestamp},
};
use gateway_framework::{
    auth::AuthToken,
    blocks::Block,
    budgets::USD,
    chains::ChainReader,
    errors::{
        Error, IndexerError,
        UnavailableReason::{self, *},
    },
    network::{
        discovery::Status,
        indexing_performance::{IndexingPerformance, Snapshot},
    },
    reporting::{with_metric, KafkaClient, CLIENT_REQUEST_TARGET, INDEXER_REQUEST_TARGET, METRICS},
    scalar::{ReceiptStatus, ScalarReceipt},
    topology::network::{Deployment, GraphNetwork, Subgraph},
};
use headers::ContentType;
use indexer_selection::{ArrayVec, Candidate, Normalized};
use num_traits::cast::ToPrimitive as _;
use ordered_float::NotNan;
use prost::bytes::Buf;
use rand::{rngs::SmallRng, Rng as _, SeedableRng as _};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thegraph_core::types::{attestation, DeploymentId};
use thegraph_graphql_http::http::response::{Error as GQLError, ResponseBody as GQLResponseBody};
use tokio::sync::mpsc;
use tracing::Instrument;
use url::Url;

use self::{
    attestation_header::GraphAttestation, context::Context, l2_forwarding::forward_request_to_l2,
    query_selector::QuerySelector, query_settings::QuerySettings,
};
use crate::{
    block_constraints::{resolve_block_requirements, rewrite_query, BlockRequirements},
    indexer_client::{check_block_error, IndexerClient, ResponsePayload},
    reports::{self, serialize_attestation},
    unattestable_errors::{miscategorized_attestable, miscategorized_unattestable},
};

mod attestation_header;
pub mod context;
mod l2_forwarding;
mod query_selector;
mod query_settings;

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
            target: CLIENT_REQUEST_TARGET,
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
    tracing::info!(target: CLIENT_REQUEST_TARGET, subgraph_chain);

    let manifest_min_block = deployments.last().unwrap().manifest.min_block;
    let chain = ctx.chains.chain(&subgraph_chain).await;

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
        target: CLIENT_REQUEST_TARGET,
        query = %payload.query,
        %variables,
    );

    let grt_per_usd = *ctx.grt_per_usd.borrow();
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
        target: CLIENT_REQUEST_TARGET,
        query_count = 1,
        budget_grt = (budget as f64 * 1e-18) as f32,
    );

    let versions_behind: BTreeMap<DeploymentId, u8> = deployments
        .iter()
        .rev()
        .enumerate()
        .map(|(index, deployment)| (deployment.id, index.try_into().unwrap_or(u8::MAX)))
        .collect();

    let (chain_head, blocks_per_minute, block_requirements) = {
        let chain = chain.read().await;
        let block_requirements = resolve_block_requirements(&chain, &context, manifest_min_block)?;
        let blocks_per_minute = chain.blocks_per_minute();
        let chain_head = chain.latest().map(|b| b.number);
        (chain_head, blocks_per_minute, block_requirements)
    };
    let indexing_statuses = ctx.indexing_statuses.value_immediate().unwrap();
    let chain_head = chain_head.unwrap_or_else(|| {
        available_indexers
            .iter()
            .flat_map(|indexing| indexing_statuses.get(indexing).map(|status| status.block))
            .max()
            .unwrap_or(0) // doesn't matter if no indexers have status
    });
    tracing::debug!(chain_head, blocks_per_minute, ?block_requirements);

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
        return Err(Error::BadIndexers(indexer_errors));
    }

    let mut total_indexer_fees_grt: u128 = 0;
    for retry in 0..ctx.indexer_selection_retry_limit {
        // Make sure our observations are up-to-date if retrying.
        if retry > 0 {
            ctx.indexing_perf.flush().await;

            // Update candidate performance.
            let perf_snapshots = ctx.indexing_perf.latest();
            for candidate in &mut candidates {
                let indexing = Indexing {
                    indexer: candidate.indexer,
                    deployment: candidate.deployment,
                };
                if let Some(updated) = perf_snapshots.get(&indexing).and_then(|snapshot| {
                    perf(snapshot, &block_requirements, chain_head, blocks_per_minute)
                }) {
                    candidate.perf = updated.response;
                    candidate.seconds_behind = updated.seconds_behind;
                }
            }
        }

        let selected_candidates: ArrayVec<&Candidate, SELECTION_LIMIT> =
            indexer_selection::select(&candidates);
        let selections_len = selected_candidates.len();
        let mut selections: Vec<Selection> = Default::default();
        for candidate in selected_candidates {
            let indexing = Indexing {
                indexer: candidate.indexer,
                deployment: candidate.deployment,
            };

            // over-pay indexers to hit target
            let min_fee = *ctx.budgeter.min_indexer_fees.borrow();
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
                chain: chain.clone(),
                attestation_domain: ctx.attestation_domain,
                indexing_perf: ctx.indexing_perf.clone(),
                deployment,
                response_time: Duration::default(),
            };

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
            let indexer_request = {
                let chain = chain.read().await;
                rewrite_query(
                    &chain,
                    context.clone(),
                    &block_requirements,
                    selection.blocks_behind,
                )?
            };

            total_indexer_fees_grt += selection.receipt.grt_value();

            let indexer_query_context = indexer_query_context.clone();
            let outcome_tx = outcome_tx.clone();
            // We must manually construct this span before the spawned task, since otherwise
            // there's a race between creating this span and another indexer responding which will
            // close the outer client_query span.
            let span = tracing::info_span!(
                target: INDEXER_REQUEST_TARGET,
                "indexer_request",
                indexer = ?selection.indexing.indexer,
            );
            let receipt_signer = ctx.receipt_signer;
            tokio::spawn(
                async move {
                    let response =
                        handle_indexer_query(indexer_query_context, &selection, indexer_request)
                            .await;
                    let receipt_status = match &response {
                        Ok(_) => ReceiptStatus::Success,
                        Err(IndexerError::Timeout) => ReceiptStatus::Unknown,
                        Err(_) => ReceiptStatus::Failure,
                    };
                    receipt_signer
                        .record_receipt(&selection.indexing, &selection.receipt, receipt_status)
                        .await;

                    let _ = outcome_tx.send((selection, response)).await;
                }
                .instrument(span),
            );
        }
        // This must be dropped to ensure the `outcome_rx.recv()` loop below can eventyually stop.
        drop(outcome_tx);

        let total_indexer_fees_usd =
            USD(NotNan::new(total_indexer_fees_grt as f64 * 1e-18).unwrap() / grt_per_usd);
        tracing::info!(
            target: CLIENT_REQUEST_TARGET,
            indexer_fees_grt = (total_indexer_fees_grt as f64 * 1e-18) as f32,
            indexer_fees_usd = *total_indexer_fees_usd.0 as f32,
        );

        while let Some((selection, result)) = outcome_rx.recv().await {
            match result {
                Err(err) => {
                    indexer_errors.insert(selection.indexing.indexer, err);
                }
                Ok(outcome) => {
                    let _ = ctx.budgeter.feedback.send(total_indexer_fees_usd);

                    tracing::debug!(?indexer_errors);
                    return Ok((selection, outcome));
                }
            };
        }
    }

    Err(Error::BadIndexers(indexer_errors))
}

#[allow(clippy::too_many_arguments)]
fn prepare_candidate(
    network: &GraphNetwork,
    statuses: &HashMap<Indexing, Status>,
    perf_snapshots: &HashMap<Indexing, Snapshot>,
    versions_behind: &BTreeMap<DeploymentId, u8>,
    context: &mut AgoraContext<String>,
    block_requirements: &BlockRequirements,
    chain_head: BlockNumber,
    blocks_per_minute: u64,
    budget: u128,
    indexing: Indexing,
) -> Result<Candidate, IndexerError> {
    let info = network
        .indexing(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let status = statuses
        .get(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let perf = perf_snapshots
        .get(&indexing)
        .and_then(|snapshot| perf(snapshot, block_requirements, chain_head, blocks_per_minute))
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;

    let fee = Normalized::new(indexer_fee(&status.cost_model, context)? as f64 / budget as f64)
        .unwrap_or(Normalized::ONE);

    if let Some((min, max)) = &block_requirements.range {
        // Allow indexers if their last reported block is "close enough" to the required block
        // range. This is to compensate for the gateway's lack of knowledge about which blocks
        // indexers have responded with already. All else being equal, indexers closer to chain head
        // and with higher success rate will be favored.
        let latest_block = status.block.max(perf.latest_block + blocks_per_minute);
        let range = status.min_block.unwrap_or(0)..=latest_block;
        let number_gte = block_requirements.number_gte.unwrap_or(0);
        if !range.contains(min) || !range.contains(max) || (*range.end() < number_gte) {
            return Err(IndexerError::Unavailable(UnavailableReason::MissingBlock));
        }
    }

    Ok(Candidate {
        indexer: indexing.indexer,
        deployment: indexing.deployment,
        url: info.url.clone(),
        perf: perf.response,
        fee,
        seconds_behind: perf.seconds_behind,
        slashable_grt: (info.staked_tokens as f64 * 1e-18) as u64,
        versions_behind: *versions_behind.get(&indexing.deployment).unwrap_or(&0),
        zero_allocation: info.allocated_tokens == 0,
    })
}

struct Perf {
    response: indexer_selection::ExpectedPerformance,
    latest_block: BlockNumber,
    seconds_behind: u32,
}

fn perf(
    snapshot: &Snapshot,
    block_requirements: &BlockRequirements,
    chain_head: BlockNumber,
    blocks_per_minute: u64,
) -> Option<Perf> {
    let latest_block = snapshot.latest_block?;
    let seconds_behind = if !block_requirements.latest || (blocks_per_minute == 0) {
        0
    } else {
        ((chain_head.saturating_sub(latest_block) as f64 / blocks_per_minute as f64) * 60.0).ceil()
            as u32
    };
    Some(Perf {
        response: snapshot.response.expected_performance(),
        latest_block,
        seconds_behind,
    })
}

#[derive(Clone)]
struct IndexerQueryContext {
    pub indexer_client: IndexerClient,
    pub kafka_client: &'static KafkaClient,
    pub chain: ChainReader,
    pub attestation_domain: &'static Eip712Domain,
    pub indexing_perf: IndexingPerformance,
    pub deployment: Arc<Deployment>,
    pub response_time: Duration,
}

async fn handle_indexer_query(
    mut ctx: IndexerQueryContext,
    selection: &Selection,
    indexer_request: String,
) -> Result<ResponsePayload, IndexerError> {
    let indexing = selection.indexing;
    let deployment = indexing.deployment.to_string();

    let result = handle_indexer_query_inner(&mut ctx, selection, indexer_request).await;
    METRICS.indexer_query.check(&[&deployment], &result);

    let (result, latest_block) = match result {
        Ok((result, block)) => (Ok(result), block.map(|b| b.number)),
        Err(ExtendedIndexerError {
            error,
            latest_block,
        }) => (Err(error), latest_block),
    };

    let latency_ms = ctx.response_time.as_millis() as u16;
    tracing::info!(
        target: INDEXER_REQUEST_TARGET,
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
        .feedback(indexing, result.is_ok(), latency_ms, latest_block);

    result
}

struct ExtendedIndexerError {
    error: IndexerError,
    latest_block: Option<BlockNumber>,
}

impl From<IndexerError> for ExtendedIndexerError {
    fn from(value: IndexerError) -> Self {
        Self {
            error: value,
            latest_block: None,
        }
    }
}

async fn handle_indexer_query_inner(
    ctx: &mut IndexerQueryContext,
    selection: &Selection,
    indexer_request: String,
) -> Result<(ResponsePayload, Option<Block>), ExtendedIndexerError> {
    let start_time = Instant::now();
    let result = ctx
        .indexer_client
        .query_indexer(selection, indexer_request.clone())
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

    let (client_response, errors, block) = rewrite_response(&response.payload.body)?;

    if let Some(block) = &block {
        ctx.chain.notify(block.clone(), selection.indexing.indexer);
    }

    let errors_repr = errors
        .iter()
        .map(|err| err.message.as_str())
        .collect::<Vec<&str>>()
        .join("; ");
    tracing::info!(
        target: INDEXER_REQUEST_TARGET,
        indexer_errors = errors_repr,
    );

    errors
        .iter()
        .try_for_each(|err| check_block_error(&err.message))
        .map_err(|block_error| ExtendedIndexerError {
            error: IndexerError::Unavailable(MissingBlock),
            latest_block: block_error.latest_block,
        })?;

    for error in &errors {
        if miscategorized_unattestable(&error.message) {
            let message = if !errors.is_empty() {
                format!("unattestable response: {}", errors_repr)
            } else {
                "unattestable response".to_string()
            };
            return Err(IndexerError::BadResponse(message).into());
        }
    }

    if response.payload.attestation.is_none() {
        // TODO: This is a temporary hack to handle errors that were previously miscategorized as
        //  unattestable in graph-node.
        for error in &errors {
            if miscategorized_attestable(&error.message) {
                return Ok((response.payload, block));
            }
        }

        let message = if !errors.is_empty() {
            format!("no attestation: {errors_repr}")
        } else {
            "no attestation".to_string()
        };
        return Err(IndexerError::BadResponse(message).into());
    }

    if let Some(attestation) = &response.payload.attestation {
        let allocation = selection.receipt.allocation();
        let verified = attestation::verify(
            ctx.attestation_domain,
            attestation,
            &allocation,
            &indexer_request,
            &response.payload.body,
        );
        // We send the Kafka message directly to avoid passing the request & response payloads
        // through the normal reporting path. This is to reduce log bloat.
        let payload = serialize_attestation(
            attestation,
            allocation,
            indexer_request,
            response.payload.body,
        );
        ctx.kafka_client.send("gateway_attestations", &payload);
        if let Err(err) = verified {
            return Err(
                IndexerError::BadResponse(anyhow!("bad attestation: {err}").to_string()).into(),
            );
        }
    }

    let client_response = ResponsePayload {
        body: client_response,
        attestation: response.payload.attestation,
    };
    Ok((client_response, block))
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

fn rewrite_response(
    response: &str,
) -> Result<(String, Vec<GQLError>, Option<Block>), IndexerError> {
    #[derive(Deserialize, Serialize)]
    struct ProbedData {
        #[serde(rename = "_gateway_probe_", skip_serializing)]
        probe: Option<Meta>,
        #[serde(flatten)]
        data: serde_json::Value,
    }
    #[derive(Deserialize)]
    struct Meta {
        block: MaybeBlock,
    }
    #[derive(Deserialize)]
    struct MaybeBlock {
        number: BlockNumber,
        hash: BlockHash,
        timestamp: Option<u64>,
    }
    let mut payload: GQLResponseBody<ProbedData> =
        serde_json::from_str(response).map_err(|err| IndexerError::BadResponse(err.to_string()))?;

    // Avoid processing oversized errors.
    for err in &mut payload.errors {
        err.message.truncate(256);
        err.message.shrink_to_fit();
    }

    let block = payload
        .data
        .as_mut()
        .and_then(|data| data.probe.take())
        .and_then(|meta| {
            Some(Block {
                number: meta.block.number,
                hash: meta.block.hash,
                timestamp: meta.block.timestamp?,
            })
        });
    let client_response = serde_json::to_string(&payload).unwrap();
    Ok((client_response, payload.errors, block))
}

#[cfg(test)]
mod tests {
    mod require_req_auth {
        use std::{collections::HashMap, sync::Arc};

        use assert_matches::assert_matches;
        use axum::{
            body::Body,
            http::{Method, Request, StatusCode},
            middleware,
            routing::post,
            Extension, Router,
        };
        use gateway_framework::{
            auth::{methods::api_keys::APIKey, AuthContext, AuthToken},
            http::middleware::{legacy_auth_adapter, RequireAuthorizationLayer},
        };
        use headers::{Authorization, ContentType, HeaderMapExt};
        use http_body_util::BodyExt;
        use tokio::sync::watch;
        use tower::ServiceExt;

        /// Create a test authorization context.
        fn test_auth_ctx(key: Option<&str>) -> AuthContext {
            let mut ctx = AuthContext {
                payment_required: false,
                api_keys: watch::channel(Default::default()).1,
                special_api_keys: Default::default(),
                special_query_key_signers: Default::default(),
                subscriptions: watch::channel(Default::default()).1,
                subscription_rate_per_query: 0,
                subscription_domains: Default::default(),
            };
            if let Some(key) = key {
                ctx.api_keys = watch::channel(HashMap::from([(
                    key.into(),
                    Arc::new(APIKey {
                        key: key.into(),
                        ..Default::default()
                    }),
                )]))
                .1;
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
                    AuthToken::ApiKey(auth_token) => auth_token.key().to_string(),
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
            body: &mut Body,
        ) -> serde_json::Result<thegraph_graphql_http::http::response::ResponseBody<T>>
        where
            for<'de> T: serde::Deserialize<'de>,
        {
            let body = body.collect().await.expect("valid body").to_bytes();
            serde_json::from_slice(body.as_ref())
        }

        /// Parse text response body.
        async fn parse_text_response_body(body: &mut Body) -> anyhow::Result<String> {
            let body = body.collect().await.expect("valid body").to_bytes();
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
