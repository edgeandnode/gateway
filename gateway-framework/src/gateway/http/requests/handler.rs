use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::Address;
use anyhow::anyhow;
use axum::{
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Uri},
    response::Response,
    Extension,
};
use indexer_selection::{ArrayVec, Candidate};
use ordered_float::NotNan;
use rand::{thread_rng, Rng};
use thegraph_core::types::DeploymentId;
use tokio::sync::mpsc;
use tracing::Instrument;

use super::{
    auth::resolve_and_authorize_deployments, blocks::BlockRequirements, selector::RequestSelector,
};
use crate::{
    auth::AuthSettings,
    budgets::USD,
    chains::ChainReader,
    errors::{
        Error, IndexerError,
        UnavailableReason::{self, *},
    },
    gateway::http::{
        gateway::{DeterministicRequest, IndexerResponse},
        requests::{
            blocks::resolve_block_requirements, budget::Budget, candidates::prepare_candidate,
        },
        Gateway, GatewayImpl, GatewayState, IndexingStatus,
    },
    http::middleware::RequestId,
    indexing::Indexing,
    metrics::{with_metric, METRICS},
    reports,
    topology::network::{Deployment, Manifest},
};

const SELECTION_LIMIT: usize = 3;

pub struct GatewayRequestContext {
    pub auth: AuthSettings,
    pub original_uri: Uri,
    pub selector: RequestSelector,
}

pub struct IncomingRequest {
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl<G> Gateway<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    pub async fn handle_request(
        State(state): State<Arc<GatewayState<G>>>,
        Extension(auth): Extension<AuthSettings>,
        Extension(RequestId(request_id)): Extension<RequestId>,
        OriginalUri(original_uri): OriginalUri,
        selector: RequestSelector,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response<String> {
        let start_time = Instant::now();

        let (deployments, subgraph) =
            match resolve_and_authorize_deployments(&state.network, &auth, &selector) {
                Ok((deployments, subgraphs)) => (deployments, subgraphs),
                Err(err) => {
                    return state.gateway_impl.finalize_response(Err(err)).await;
                }
            };

        if let Some(l2_url) = state.l2_gateway.as_ref() {
            // Forward query to L2 gateway if it's marked as transferred & there are no
            // allocations. abf62a6d-c071-4507-b528-ddc8e250127a
            if deployments.iter().all(|d| d.transferred_to_l2) {
                return state
                    .gateway_impl
                    .forward_request_to_l2(
                        l2_url,
                        &original_uri,
                        headers,
                        body,
                        subgraph.and_then(|s| s.l2_id),
                    )
                    .await;
            }
        }

        let available_indexers: BTreeSet<Indexing> = deployments
            .iter()
            .flat_map(move |deployment| {
                let id = deployment.id;
                deployment.indexers.keys().map(move |indexer| Indexing {
                    indexer: *indexer,
                    deployment: id,
                })
            })
            .collect();
        if available_indexers.is_empty() {
            return state
                .gateway_impl
                .finalize_response(Err(Error::NoIndexers))
                .await;
        }

        let manifest = match deployments
            .last()
            .map(|deployment| deployment.manifest.clone())
            .ok_or_else(|| Error::SubgraphNotFound(anyhow!("no matching deployments")))
        {
            Ok(manifest) => manifest,
            Err(err) => {
                return state.gateway_impl.finalize_response(Err(err)).await;
            }
        };

        let client_request_ctx = GatewayRequestContext {
            auth,
            original_uri,
            selector,
        };

        let budget = match Budget::calculate(
            &state.grt_per_usd,
            &state.budgeter,
            &client_request_ctx.auth,
        ) {
            Ok(budget) => budget,
            Err(err) => {
                return state.gateway_impl.finalize_response(Err(err)).await;
            }
        };

        let client_request = match state
            .gateway_impl
            .parse_request(&client_request_ctx, &IncomingRequest { headers, body })
            .await
        {
            Ok(client_request) => client_request,
            Err(err) => {
                return state.gateway_impl.finalize_response(Err(err)).await;
            }
        };

        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(
            run_indexer_requests(
                state.clone(),
                request_id,
                start_time,
                deployments,
                available_indexers,
                manifest,
                budget,
                client_request_ctx,
                client_request,
                tx,
            )
            .in_current_span(),
        );
        let result = rx.recv().await.unwrap();
        drop(rx);

        match &result {
            Ok(_) => METRICS.client_query.ok.inc(),
            Err(_) => METRICS.client_query.err.inc(),
        };
        METRICS
            .client_query
            .duration
            .observe(Instant::now().duration_since(start_time).as_secs_f64());

        state.gateway_impl.finalize_response(result).await
    }
}

async fn run_indexer_requests<G>(
    state: Arc<GatewayState<G>>,
    request_id: String,
    start_time: Instant,
    deployments: Vec<Arc<Deployment>>,
    mut available_indexers: BTreeSet<Indexing>,
    manifest: Manifest,
    budget: Budget,
    client_request_ctx: GatewayRequestContext,
    client_request: G::Request,
    client_response: mpsc::Sender<Result<IndexerResponse, Error>>,
) where
    G: GatewayImpl,
{
    let chain = state.chains.chain(&manifest.network).await;
    let chain_reader = chain.read().await;
    let blocks_per_minute = chain_reader.blocks_per_minute();
    let chain_head = chain_reader.latest().map(|b| b.number);

    let block_constraints = state
        .gateway_impl
        .block_constraints(&client_request_ctx, &client_request)
        .await
        .unwrap_or_default();
    let block_requirements =
        match resolve_block_requirements(&chain_reader, &block_constraints, manifest.min_block) {
            Ok(block_requirements) => block_requirements,
            Err(err) => {
                client_response
                    .try_send(Err(Error::BadQuery(anyhow!("{err}"))))
                    .unwrap();
                return;
            }
        };
    drop(chain_reader);

    let indexing_statuses = state.indexing_statuses.value_immediate().unwrap();
    let chain_head = chain_head.unwrap_or_else(|| {
        available_indexers
            .iter()
            .flat_map(|indexing| indexing_statuses.get(indexing).map(|status| status.block()))
            .max()
            .unwrap_or(0) // doesn't matter if no indexers have status
    });
    tracing::debug!(chain_head, blocks_per_minute, ?block_requirements);

    let mut indexer_errors: BTreeMap<Address, IndexerError> = Default::default();
    let blocklist = state
        .indexings_blocklist
        .value_immediate()
        .unwrap_or_default();
    available_indexers.retain(|candidate| {
        if blocklist.contains(candidate) || state.bad_indexers.contains(&candidate.indexer) {
            indexer_errors.insert(
                candidate.indexer,
                IndexerError::Unavailable(UnavailableReason::NoStatus),
            );
            return false;
        }
        true
    });

    // List holding the indexers that support Scalar TAP.
    //
    // This is a temporary solution determine which indexers support Scalar TAP. This will be
    // removed once the network service is integrated.
    let mut indexers_with_tap_support = HashSet::new();

    let versions_behind: BTreeMap<DeploymentId, u8> = deployments
        .iter()
        .rev()
        .enumerate()
        .map(|(index, deployment)| (deployment.id, index.try_into().unwrap_or(u8::MAX)))
        .collect();

    let mut candidates = Vec::new();
    {
        let perf = state.indexing_performance.latest();
        for indexing in available_indexers {
            if let Some(status) = indexing_statuses.get(&indexing) {
                // If the indexer status indicates it supports Scalar TAP, add it to the set of
                // indexers with Scalar TAP support.
                if !status.legacy_scalar() {
                    indexers_with_tap_support.insert(indexing.indexer);
                }
            }

            match prepare_candidate(
                &state.network,
                |request, status| {
                    state.clone().gateway_impl.indexer_request_fee(
                        &client_request_ctx,
                        request,
                        status,
                    )
                },
                &indexing_statuses,
                &perf,
                &versions_behind,
                &client_request,
                &block_requirements,
                chain_head,
                blocks_per_minute,
                &budget,
                indexing,
            ) {
                Ok(candidate) => candidates.push(candidate),
                Err(indexer_error) => {
                    indexer_errors.insert(indexing.indexer, indexer_error);
                }
            }
        }
    }

    if tracing::enabled!(tracing::Level::TRACE) {
        tracing::trace!(?client_request);
        tracing::trace!(?candidates);
    } else if tracing::enabled!(tracing::Level::DEBUG) && thread_rng().gen_bool(0.01) {
        tracing::debug!(?client_request);
        tracing::debug!(?candidates);
    }

    process_request_using_candidates(
        state,
        indexers_with_tap_support,
        candidates,
        chain,
        blocks_per_minute,
        request_id,
        client_request_ctx,
        client_request,
        &block_requirements,
        budget,
        start_time,
        manifest,
        indexer_errors,
        client_response,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_request_using_candidates<G>(
    state: Arc<GatewayState<G>>,
    indexers_with_tap_support: HashSet<Address>,
    mut candidates: Vec<Candidate>,
    chain: ChainReader,
    blocks_per_minute: u64,
    request_id: String,
    client_request_ctx: GatewayRequestContext,
    client_request: G::Request,
    block_requirements: &BlockRequirements,
    budget: Budget,
    start_time: Instant,
    manifest: Manifest,
    indexer_errors: BTreeMap<Address, IndexerError>,
    client_response: mpsc::Sender<Result<IndexerResponse, Error>>,
) where
    G: GatewayImpl,
{
    let client_request_ctx = Arc::new(client_request_ctx);

    let mut indexer_requests: Vec<reports::IndexerRequest> = Default::default();
    let mut indexer_request_rewrites: BTreeMap<u32, DeterministicRequest> = Default::default();
    let mut client_response_time: Option<Duration> = None;

    // If a client query cannot be handled by the available indexers, we should give a reason for
    // all of the available indexers in the `bad indexers` response.
    while !candidates.is_empty()
        && (Instant::now().duration_since(start_time) < Duration::from_secs(60))
    {
        let selections: ArrayVec<&Candidate, SELECTION_LIMIT> =
            indexer_selection::select(&candidates);
        if selections.is_empty() {
            // Candidates that would never be selected should be filtered out for improved errors.
            tracing::error!("no candidates selected");
            break;
        }

        let (tx, mut rx) = mpsc::channel(SELECTION_LIMIT);
        let min_fee = *state.budgeter.min_indexer_fees.borrow();
        for &selection in &selections {
            let indexer = selection.indexer;
            let deployment = selection.deployment;
            let url = selection.url.clone();
            let seconds_behind = selection.seconds_behind;
            let legacy_scalar = !indexers_with_tap_support.contains(&indexer);
            let subgraph_chain = manifest.network.clone();
            // let client_request_ctx = client_request_ctx.clone();

            // over-pay indexers to hit target
            let min_fee =
                *(min_fee.0 * budget.grt_per_usd * budget.one_grt) / selections.len() as f64;
            let indexer_fee = selection.fee.as_f64() * budget.budget as f64;
            let fee = indexer_fee.max(min_fee) as u128;
            let receipt = match if legacy_scalar {
                state
                    .receipt_signer
                    .create_legacy_receipt(indexer, deployment, fee)
                    .await
            } else {
                state
                    .receipt_signer
                    .create_receipt(indexer, deployment, fee)
                    .await
            } {
                Some(receipt) => receipt,
                None => {
                    tracing::error!(%indexer, %deployment, "failed to create receipt");
                    continue;
                }
            };
            debug_assert!(fee == receipt.grt_value());

            let blocks_behind = blocks_behind(seconds_behind, blocks_per_minute);
            let indexer_request = match indexer_request_rewrites.get(&seconds_behind) {
                Some(indexer_request) => indexer_request.clone(),
                None => {
                    let chain = chain.read().await;
                    let indexer_request = state.gateway_impl.deterministic_request(
                        client_request_ctx.as_ref(),
                        &client_request,
                        &chain,
                        &block_requirements,
                        blocks_behind,
                    );
                    if selections
                        .iter()
                        .filter(|s| s.seconds_behind == seconds_behind)
                        .count()
                        > 1
                    {
                        indexer_request_rewrites.insert(seconds_behind, indexer_request.clone());
                    }
                    indexer_request
                }
            };
            let tx = tx.clone();
            let state = state.clone();
            let client_request_ctx = client_request_ctx.clone();
            tokio::spawn(async move {
                let start_time = Instant::now();
                let result = state
                    .gateway_impl
                    .send_request_to_indexer(
                        client_request_ctx.as_ref(),
                        &deployment,
                        &url,
                        &receipt,
                        state.attestation_domain,
                        // FIXME: There is a small performance penality in cloning the request here
                        indexer_request.clone(),
                    )
                    .await;
                let response_time_ms = Instant::now().duration_since(start_time).as_millis() as u16;
                let report = reports::IndexerRequest {
                    indexer,
                    deployment,
                    url: url.to_string(),
                    allocation: receipt.allocation(),
                    subgraph_chain,
                    result,
                    response_time_ms,
                    seconds_behind,
                    blocks_behind,
                    legacy_scalar,
                    fee,
                    request: indexer_request,
                };
                tx.try_send(report).unwrap();
            });
        }
        drop(tx);
        while let Some(report) = rx.recv().await {
            if let Ok(response) = report.result.as_ref() {
                if client_response_time.is_none() {
                    let _ = client_response.try_send(Ok(response.clone()));
                    client_response_time = Some(Instant::now().duration_since(start_time));
                }
            }
            indexer_requests.push(report);
        }

        if client_response_time.is_some() {
            break;
        }

        let selected_indexers: ArrayVec<Address, SELECTION_LIMIT> =
            selections.into_iter().map(|s| s.indexer).collect();
        candidates.retain(|c| !selected_indexers.contains(&c.indexer));
    }
    tracing::info!(?indexer_errors);

    // Send fallback error to use when no indexers are successful.
    if client_response_time.is_none() {
        let _ = client_response.try_send(Err(Error::BadIndexers(indexer_errors.clone())));
        client_response_time = Some(Instant::now().duration_since(start_time));
    }

    let result = if indexer_requests.iter().any(|r| r.result.is_ok()) {
        Ok(())
    } else {
        Err(Error::BadIndexers(indexer_errors))
    };

    let total_fees_grt: f64 = indexer_requests.iter().map(|i| i.fee as f64 * 1e-18).sum();
    let total_fees_usd = USD(NotNan::new(total_fees_grt / *budget.grt_per_usd).unwrap());
    let _ = state.budgeter.feedback.send(total_fees_usd);

    for indexer_request in &indexer_requests {
        let latest_block = match &indexer_request.result {
            Ok(response) => response.probe_block.as_ref().map(|b| b.number),
            Err(IndexerError::Unavailable(MissingBlock(err))) => err.latest,
            _ => None,
        };
        state.indexing_performance.feedback(
            indexer_request.indexer,
            indexer_request.deployment,
            indexer_request.result.is_ok(),
            indexer_request.response_time_ms,
            latest_block,
        );

        if let Some(block) = indexer_request
            .result
            .as_ref()
            .ok()
            .and_then(|r| r.probe_block.clone())
        {
            chain.notify(block, indexer_request.indexer);
        }

        let deployment = indexer_request.deployment.to_string();
        let indexer = format!("{:?}", indexer_request.indexer);
        let labels = [deployment.as_str(), indexer.as_str()];
        METRICS
            .indexer_query
            .check(&labels, &indexer_request.result);
        with_metric(&METRICS.indexer_query.duration, &labels, |hist| {
            hist.observe(indexer_request.response_time_ms as f64)
        });

        tracing::info!(
            indexer = ?indexer_request.indexer,
            deployment = %indexer_request.deployment,
            allocation = ?indexer_request.allocation,
            url = indexer_request.url,
            result = ?indexer_request.result.as_ref().map(|_| ()),
            response_time_ms = indexer_request.response_time_ms,
            seconds_behind = indexer_request.seconds_behind,
            fee = indexer_request.fee as f64 * 1e-18,
            "indexer_request"
        );
        tracing::trace!(indexer_request = ?indexer_request.request);
    }

    let response_time_ms = client_response_time.unwrap().as_millis() as u16;
    tracing::info!(
        result = ?result,
        response_time_ms,
        total_fees_grt,
        total_fees_usd = *total_fees_usd.0,
    );

    let _ = state.reporter.send(reports::ClientRequest {
        id: request_id,
        response_time_ms,
        result,
        api_key: client_request_ctx.auth.key.clone(),
        user_address: client_request_ctx.auth.user,
        grt_per_usd: budget.grt_per_usd,
        indexer_requests,
    });
}

fn blocks_behind(seconds_behind: u32, blocks_per_minute: u64) -> u64 {
    ((seconds_behind as f64 / 60.0) * blocks_per_minute as f64) as u64
}
