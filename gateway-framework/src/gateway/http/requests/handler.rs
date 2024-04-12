use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, BlockNumber};
use anyhow::anyhow;
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode, Uri},
    response::Response,
    Extension,
};
use gateway_common::{types::Indexing, utils::timestamp::unix_timestamp};
use indexer_selection::{ArrayVec, Candidate};
use ordered_float::NotNan;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use thegraph_core::types::DeploymentId;
use tokio::sync::mpsc;
use tracing::Instrument;

use super::{auth::resolve_and_authorize_deployments, BlockRequirements, RequestSelector};
use crate::{
    auth::{AuthToken, RequestSettings},
    blocks::Block,
    budgets::USD,
    chains::ChainReader,
    errors::{Error, IndexerError},
    gateway::http::{
        gateway::{IndexerResponse, IndexingStatus, SelectionInfo},
        requests::{
            budget::Budget,
            candidates::{available_indexers_for_deployments, performance, prepare_candidate},
            resolve_block_requirements,
        },
        Gateway, GatewayImpl, GatewayState,
    },
    reporting::{with_metric, CLIENT_REQUEST_TARGET, INDEXER_REQUEST_TARGET, METRICS},
    scalar::{ReceiptStatus, ScalarReceipt},
    topology::network::Deployment,
};

const SELECTION_LIMIT: usize = 3;

pub struct GatewayRequestContext {
    pub auth: AuthToken,
    pub original_uri: Uri,
    pub selector: RequestSelector,
}

pub struct GatewayRequest {
    pub headers: HeaderMap,
    pub body: Bytes,
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

fn legacy_status<T>(result: &Result<T, Error>) -> (String, u32) {
    match result {
        Ok(_) => ("200 OK".to_string(), 0),
        Err(err) => match err {
            Error::BlockNotFound(_) => ("Unresolved block".to_string(), 604610595),
            Error::Internal(_) => ("Internal error".to_string(), 816601499),
            Error::Auth(_) => ("Invalid API key".to_string(), 888904173),
            Error::BadQuery(_) => ("Invalid query".to_string(), 595700117),
            Error::NoIndexers => (
                "No indexers found for subgraph deployment".to_string(),
                1621366907,
            ),
            Error::BadIndexers(_) => (
                "No suitable indexer found for subgraph deployment".to_string(),
                510359393,
            ),
            Error::SubgraphNotFound(_) => (err.to_string(), 2599148187),
        },
    }
}

impl<G> Gateway<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    pub async fn handle_request(
        State(state): State<Arc<GatewayState<G>>>,
        Extension(auth): Extension<AuthToken>,
        settings: Option<Extension<RequestSettings>>,
        OriginalUri(original_uri): OriginalUri,
        selector: RequestSelector,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response<Body> {
        let start_time = Instant::now();
        let timestamp = unix_timestamp();

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

        let result = handle_request_inner(
            state.clone(),
            deployments,
            auth,
            selector,
            settings
                .map(|Extension(settings)| settings)
                .unwrap_or_default(),
            original_uri,
            headers,
            body,
        )
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
            let (legacy_status_message, legacy_status_code) = legacy_status(&result);
            tracing::info!(
                target: CLIENT_REQUEST_TARGET,
                start_time_ms = timestamp,
                deployment,
                %status_message,
                %legacy_status_message,
                legacy_status_code,
            );
        }

        state.gateway_impl.finalize_response(result).await
    }
}

async fn handle_request_inner<G>(
    state: Arc<GatewayState<G>>,
    deployments: Vec<Arc<Deployment>>,
    auth: AuthToken,
    selector: RequestSelector,
    settings: RequestSettings,
    original_uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(SelectionInfo, IndexerResponse), Error>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    let subgraph_chain = deployments
        .last()
        .map(|deployment| deployment.manifest.network.clone())
        .ok_or_else(|| Error::SubgraphNotFound(anyhow!("no matching deployments")))?;

    tracing::info!(target: CLIENT_REQUEST_TARGET, subgraph_chain);

    let mut indexer_errors: BTreeMap<Address, IndexerError> = Default::default();

    let available_indexers =
        available_indexers_for_deployments(&state, &deployments, &mut indexer_errors);

    if available_indexers.is_empty() {
        return Err(Error::NoIndexers);
    }

    let budget = Budget::calculate(&state, &settings)?;

    tracing::info!(
        target: CLIENT_REQUEST_TARGET,
        query_count = 1,
        budget_grt = (budget.budget as f64 * 1e-18) as f32,
    );

    let versions_behind: BTreeMap<DeploymentId, u8> = deployments
        .iter()
        .rev()
        .enumerate()
        .map(|(index, deployment)| (deployment.id, index.try_into().unwrap_or(u8::MAX)))
        .collect();

    let manifest_min_block = deployments.last().unwrap().manifest.min_block;
    let chain = state.chains.chain(&subgraph_chain).await;

    let context = GatewayRequestContext {
        auth,
        original_uri,
        selector,
    };
    let mut request = state
        .gateway_impl
        .parse_request(&context, GatewayRequest { headers, body })
        .await?;

    let block_constraints = state
        .gateway_impl
        .block_constraints(&context, &request)
        .await
        .unwrap_or_default();

    let (chain_head, blocks_per_minute, block_requirements) = {
        let chain = chain.read().await;
        let block_requirements =
            resolve_block_requirements(&chain, &block_constraints, manifest_min_block)?;
        let blocks_per_minute = chain.blocks_per_minute();
        let chain_head = chain.latest().map(|b| b.number);
        (chain_head, blocks_per_minute, block_requirements)
    };

    let indexing_statuses = state.indexing_statuses.value_immediate().unwrap();
    let chain_head = chain_head.unwrap_or_else(|| {
        available_indexers
            .iter()
            .flat_map(|indexing| indexing_statuses.get(indexing).map(|status| status.block()))
            .max()
            .unwrap_or(0) // doesn't matter if no indexers have status
    });

    tracing::debug!(chain_head, blocks_per_minute, ?block_requirements);

    let mut candidates = Vec::new();
    {
        let performance_snapshots = state.indexing_performance.latest();
        for indexing in available_indexers {
            match prepare_candidate(
                &state,
                &context,
                &mut request,
                &indexing_statuses,
                &performance_snapshots,
                &block_requirements,
                chain_head,
                blocks_per_minute,
                &versions_behind,
                indexing,
                &budget,
            )
            .await
            {
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

    process_request_using_candidates(
        state,
        candidates,
        deployments,
        &block_requirements,
        chain,
        chain_head,
        blocks_per_minute,
        budget,
        Arc::new(context),
        request,
        indexer_errors,
    )
    .await
}

async fn process_request_using_candidates<G>(
    state: Arc<GatewayState<G>>,
    mut candidates: Vec<Candidate>,
    deployments: Vec<Arc<Deployment>>,
    block_requirements: &BlockRequirements,
    chain: ChainReader,
    chain_head: u64,
    blocks_per_minute: u64,
    budget: Budget,
    context: Arc<GatewayRequestContext>,
    request: G::Request,
    mut indexer_errors: BTreeMap<Address, IndexerError>,
) -> Result<(SelectionInfo, IndexerResponse), Error>
where
    G: GatewayImpl,
{
    let mut total_indexer_fees_grt: u128 = 0;
    for retry in 0..state.config.indexer_selection.retry_limit {
        // Make sure our observations are up-to-date if retrying.
        if retry > 0 {
            state.indexing_performance.flush().await;

            // Update candidate performance.
            let perf_snapshots = state.indexing_performance.latest();
            for candidate in &mut candidates {
                let indexing = Indexing {
                    indexer: candidate.indexer,
                    deployment: candidate.deployment,
                };
                if let Some(updated) = perf_snapshots.get(&indexing).and_then(|snapshot| {
                    performance(snapshot, &block_requirements, chain_head, blocks_per_minute)
                }) {
                    candidate.perf = updated.response;
                    candidate.seconds_behind = updated.seconds_behind;
                }
            }
        }

        let selected_candidates: ArrayVec<&Candidate, SELECTION_LIMIT> =
            indexer_selection::select(&candidates);
        let selections_len = selected_candidates.len();
        let mut selections: Vec<SelectionInfo> = Default::default();
        for candidate in selected_candidates {
            let indexing = Indexing {
                indexer: candidate.indexer,
                deployment: candidate.deployment,
            };

            // over-pay indexers to hit target
            let min_fee = state
                .budgeter
                .min_indexer_fees
                .value_immediate()
                .unwrap_or_default();
            let min_fee =
                *(min_fee.0 * budget.grt_per_usd * budget.one_grt) / selections_len as f64;
            let indexer_fee = candidate.fee.as_f64() * budget.budget as f64;
            let fee = indexer_fee.max(min_fee) as u128;

            let receipt = match state.receipt_signer.create_receipt(&indexing, fee).await {
                Some(receipt) => receipt,
                None => {
                    tracing::error!(?indexing, "failed to create receipt");
                    continue;
                }
            };
            debug_assert!(fee == receipt.grt_value());

            let blocks_behind = (candidate.seconds_behind as f64 / 60.0) * blocks_per_minute as f64;
            selections.push(SelectionInfo {
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
            let context = context.clone();
            let state = state.clone();

            let deployment = deployments
                .iter()
                .find(|deployment| deployment.id == selection.indexing.deployment)
                .unwrap()
                .clone();

            let deterministic_request = {
                let chain = chain.read().await;
                state.gateway_impl.deterministic_request(
                    context.as_ref(),
                    &request,
                    &chain,
                    &block_requirements,
                    selection.blocks_behind,
                )?
            };

            // let indexer_query_context = IndexerQueryContext {
            //     indexer_client: gateway_impl.indexer_client.clone(),
            //     kafka_client: state.kafka_client,
            //     chain: chain.clone(),
            //     attestation_domain: state.attestation_domain, -> Result<Self::DeterministicRequest, Error>;
            //     indexing_perf: state.indexing_performance.clone(),
            //     deployment,
            //     response_time: Duration::default(),
            // };

            // // The Agora context must be cloned to preserve the state of the original client query.
            // // This to avoid the following scenario:
            // // 1. A client query has no block requirements set for some top-level operation
            // // 2. The first indexer is selected, with some indexing status at block number `n`
            // // 3. The query is made deterministic by setting the block requirement to the hash of
            // //    block `n`
            // // 4. Some condition requires us to retry this query on another indexer with an indexing
            // //    status at a block less than `n`
            // // 5. The same context is re-used, including the block requirement set to the hash of
            // //    block `n`
            // // 6. The indexer is seen as being behind and is unnecessarily penalized

            total_indexer_fees_grt += selection.receipt.grt_value();

            // let indexer_query_context = indexer_query_context.clone();
            let outcome_tx = outcome_tx.clone();
            // We must manually construct this span before the spawned task, since otherwise
            // there's a race between creating this span and another indexer responding which will
            // close the outer client_query span.
            let span = tracing::info_span!(
                target: INDEXER_REQUEST_TARGET,
                "indexer_request",
                indexer = ?selection.indexing.indexer,
            );
            let receipt_signer = state.receipt_signer;
            let chain = chain.clone();
            tokio::spawn(
                async move {
                    let response = process_request_using_indexer(
                        state.as_ref(),
                        chain,
                        context,
                        &selection,
                        deployment,
                        deterministic_request,
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

                    let _ = outcome_tx.send((selection, response)).await;
                }
                .instrument(span),
            );
        }
        // This must be dropped to ensure the `outcome_rx.recv()` loop below can eventyually stop.
        drop(outcome_tx);

        let total_indexer_fees_usd =
            USD(NotNan::new(total_indexer_fees_grt as f64 * 1e-18).unwrap() / budget.grt_per_usd);
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
                    let _ = state.budgeter.feedback.send(total_indexer_fees_usd);

                    tracing::debug!(?indexer_errors);
                    return Ok((selection, outcome));
                }
            };
        }
    }

    Err(Error::BadIndexers(indexer_errors))
}

async fn process_request_using_indexer<G>(
    state: &GatewayState<G>,
    chain: ChainReader,
    context: Arc<GatewayRequestContext>,
    selection: &SelectionInfo,
    deployment: Arc<Deployment>,
    request: G::DeterministicRequest,
) -> Result<IndexerResponse, IndexerError>
where
    G: GatewayImpl,
{
    let indexing = selection.indexing;
    let deployment_id = indexing.deployment.to_string();

    let mut response_time = Duration::default();
    let result = process_request_using_indexer_inner(
        &state,
        context.as_ref(),
        selection,
        chain,
        request,
        &mut response_time,
    )
    .await;

    METRICS.indexer_query.check(&[&deployment_id], &result);

    let (result, latest_block) = match result {
        Ok((result, block)) => (Ok(result), block.map(|b| b.number)),
        Err(ExtendedIndexerError {
            error,
            latest_block,
        }) => (Err(error), latest_block),
    };

    let latency_ms = response_time.as_millis() as u32;
    tracing::info!(
        target: INDEXER_REQUEST_TARGET,
        deployment = %deployment_id,
        url = %selection.url,
        blocks_behind = selection.blocks_behind,
        fee_grt = (selection.receipt.grt_value() as f64 * 1e-18) as f32,
        allocation = ?selection.receipt.allocation(),
        legacy_scalar = matches!(&selection.receipt, ScalarReceipt::Legacy(_, _)),
        subgraph_chain = %deployment.manifest.network,
        response_time_ms = latency_ms,
        status_message = match &result {
            Ok(_) => "200 OK".to_string(),
            Err(err) => format!("{err:?}"),
        },
        status_code = indexer_error_status_code(result.as_ref().err()),
    );

    state
        .indexing_performance
        .feedback(indexing, result.is_ok(), latency_ms, latest_block);

    result
}

// Like much of this file. This is maintained is a partially backward-compatible state for data
// science, and should be deleted ASAP.
fn indexer_error_status_code(error: Option<&IndexerError>) -> u32 {
    let (prefix, data) = match error {
        None => (0x0, 200_u32.to_be()),
        Some(IndexerError::Internal(_)) => (0x1, 0x0),
        Some(IndexerError::Unavailable(_)) => (0x2, 0x0),
        Some(IndexerError::Timeout) => (0x3, 0x0),
        Some(IndexerError::BadResponse(_)) => (0x4, 0x0),
    };
    (prefix << 28) | (data & (u32::MAX >> 4))
}

async fn process_request_using_indexer_inner<G>(
    state: &GatewayState<G>,
    context: &GatewayRequestContext,
    selection: &SelectionInfo,
    chain: ChainReader,
    request: G::DeterministicRequest,
    response_time: &mut Duration,
) -> Result<(IndexerResponse, Option<Block>), ExtendedIndexerError>
where
    G: GatewayImpl,
{
    let start_time = Instant::now();
    let result = state
        .gateway_impl
        .send_request_to_indexer(context, &selection, request)
        .await;
    *response_time = Instant::now() - start_time;

    let deployment = selection.indexing.deployment.to_string();
    with_metric(&METRICS.indexer_query.duration, &[&deployment], |hist| {
        hist.observe(response_time.as_millis() as f64)
    });

    let response = result?;
    if response.status != StatusCode::OK.as_u16() {
        tracing::warn!(indexer_response_status = %response.status);
    }

    todo!("continue here");

    // let (client_response, errors, block) = rewrite_response(&response.payload.body)?;

    // if let Some(block) = &block {
    //     chain.notify(block.clone(), selection.indexing.indexer);
    // }

    // let errors_repr = errors
    //     .iter()
    //     .map(|err| err.message.as_str())
    //     .collect::<Vec<&str>>()
    //     .join("; ");
    // tracing::info!(
    //     target: INDEXER_REQUEST_TARGET,
    //     indexer_errors = errors_repr,
    // );

    // errors
    //     .iter()
    //     .try_for_each(|err| check_block_error(&err.message))
    //     .map_err(|block_error| ExtendedIndexerError {
    //         error: IndexerError::Unavailable(UnavailableReason::MissingBlock),
    //         latest_block: block_error.latest_block,
    //     })?;

    // for error in &errors {
    //     if miscategorized_unattestable(&error.message) {
    //         let message = if !errors.is_empty() {
    //             format!("unattestable response: {}", errors_repr)
    //         } else {
    //             "unattestable response".to_string()
    //         };
    //         return Err(IndexerError::BadResponse(message).into());
    //     }
    // }

    // if response.attestation.is_none() {
    //     // TODO: This is a temporary hack to handle errors that were previously miscategorized as
    //     //  unattestable in graph-node.
    //     for error in &errors {
    //         if miscategorized_attestable(&error.message) {
    //             return Ok((response.payload, block));
    //         }
    //     }

    //     let message = if !errors.is_empty() {
    //         format!("no attestation: {errors_repr}")
    //     } else {
    //         "no attestation".to_string()
    //     };
    //     return Err(IndexerError::BadResponse(message).into());
    // }

    // if let Some(attestation) = &response.payload.attestation {
    //     let allocation = selection.receipt.allocation();
    //     let verified = attestation::verify(
    //         ctx.attestation_domain,
    //         attestation,
    //         &allocation,
    //         &indexer_request,
    //         &response.payload.body,
    //     );
    //     // We send the Kafka message directly to avoid passing the request & response payloads
    //     // through the normal reporting path. This is to reduce log bloat.
    //     let payload = serialize_attestation(
    //         attestation,
    //         allocation,
    //         indexer_request,
    //         response.payload.body,
    //     );
    //     ctx.kafka_client.send("gateway_attestations", &payload);
    //     if let Err(err) = verified {
    //         return Err(
    //             IndexerError::BadResponse(anyhow!("bad attestation: {err}").to_string()).into(),
    //         );
    //     }
    // }

    // let client_response = ResponsePayload {
    //     body: client_response,
    //     attestation: response.payload.attestation,
    // };
    // Ok((client_response, block))
}
