use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use alloy_primitives::BlockNumber;
use anyhow::anyhow;
use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, Response, StatusCode},
    Extension,
};
use cost_model::{Context as AgoraContext, CostModel};
use custom_debug::CustomDebug;
use gateway_common::{http_ext::HttpBuilderExt as _, ptr::Ptr};
use gateway_framework::{
    auth::AuthSettings,
    budgets::USD,
    errors::{Error, IndexerError, IndexerErrors, MissingBlockError, UnavailableReason},
    http::middleware::RequestId,
    metrics::{with_metric, METRICS},
};
use headers::ContentType;
use indexer_selection::{ArrayVec, Candidate, Normalized};
use num_traits::cast::ToPrimitive as _;
use ordered_float::NotNan;
use prost::bytes::Buf;
use rand::{thread_rng, Rng as _};
use serde::Deserialize;
use serde_json::value::RawValue;
use thegraph_core::types::{AllocationId, DeploymentId, IndexerId, SubgraphId};
use tokio::sync::mpsc;
use tracing::{info_span, Instrument as _};
use url::Url;

use self::{
    attestation_header::GraphAttestation, context::Context, l2_forwarding::forward_request_to_l2,
    query_selector::QuerySelector, query_settings::QuerySettings,
};
use crate::{
    block_constraints::{resolve_block_requirements, rewrite_query, BlockRequirements},
    indexer_client::IndexerResponse,
    indexing_performance,
    network::{self, DeploymentError, Indexing, IndexingId, ResolvedSubgraphInfo, SubgraphError},
    receipts::ReceiptStatus,
    reports,
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

#[allow(clippy::too_many_arguments)]
pub async fn handle_query(
    State(ctx): State<Context>,
    Extension(auth): Extension<AuthSettings>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    query_settings: Option<Extension<QuerySettings>>,
    OriginalUri(original_uri): OriginalUri,
    selector: QuerySelector,
    headers: HeaderMap,
    payload: Bytes,
) -> Result<Response<String>, Error> {
    let start_time = Instant::now();

    // Check if the query selector is authorized by the auth token and
    // resolve the subgraph deployments for the query.
    let subgraph = match resolve_subgraph_info(&ctx, &auth, selector).await? {
        Err(ResolutionError::TransferredToL2 { id_on_l2 }) => {
            return match ctx.l2_gateway.as_ref() {
                Some(l2_gateway_url) => Ok(forward_request_to_l2(
                    &ctx.indexer_client.client,
                    l2_gateway_url,
                    &original_uri,
                    headers,
                    payload,
                    id_on_l2,
                )
                .await),
                None => Err(Error::SubgraphNotFound(anyhow!("transferred to l2"))),
            }
        }
        Ok(info) => info,
    };

    let client_request: QueryBody =
        serde_json::from_reader(payload.reader()).map_err(|err| Error::BadQuery(err.into()))?;

    // Calculate the budget for the query
    let grt_per_usd = *ctx.grt_per_usd.borrow();
    let one_grt = NotNan::new(1e18).unwrap();
    let budget = {
        let mut budget = *(ctx.budgeter.query_fees_target.0 * grt_per_usd * one_grt) as u128;
        if let Some(Extension(QuerySettings {
            budget_usd: Some(user_budget_usd),
        })) = query_settings
        {
            // Security: Consumers can and will set their budget to unreasonably high values.
            // This `.min` prevents the budget from being set far beyond what it would be
            // automatically. The reason this is important is that sometimes queries are
            // subsidized, and we would be at-risk to allow arbitrarily high values.
            let max_budget = budget * 10;

            budget = (*(user_budget_usd * grt_per_usd * one_grt) as u128).min(max_budget);
        }
        budget
    };

    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(
        run_indexer_queries(
            ctx,
            request_id,
            auth,
            start_time,
            subgraph,
            budget,
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
        .observe(start_time.elapsed().as_secs_f64());

    result.map(
        |IndexerResponse {
             client_response,
             attestation,
             ..
         }| {
            Response::builder()
                .status(StatusCode::OK)
                .header_typed(ContentType::json())
                .header_typed(GraphAttestation(attestation))
                .body(client_response)
                .unwrap()
        },
    )
}

/// Error type for the `resolve_subgraph_info` function.
#[derive(Debug, thiserror::Error)]
enum ResolutionError {
    /// The subgraph (or deployment) was transferred to L2.
    #[error("subgraph transferred to L2: {id_on_l2:?}")]
    TransferredToL2 { id_on_l2: Option<SubgraphId> },
}

/// Resolve the subgraph info for the given query selector.
///
/// This function checks if the subgraph (or deployment) is authorized by the auth settings and
/// resolves the subgraph deployments associated with the query selector.
async fn resolve_subgraph_info(
    ctx: &Context,
    auth: &AuthSettings,
    selector: QuerySelector,
) -> Result<Result<ResolvedSubgraphInfo, ResolutionError>, Error> {
    match selector {
        QuerySelector::Subgraph(ref id) => {
            // If the subgraph is not authorized, return an error.
            if !auth.is_subgraph_authorized(id) {
                return Err(Error::Auth(anyhow!("subgraph not authorized by user")));
            }

            match ctx.network.resolve_with_subgraph_id(id) {
                Err(SubgraphError::TransferredToL2 { id_on_l2 }) => {
                    Ok(Err(ResolutionError::TransferredToL2 { id_on_l2 }))
                }
                Err(SubgraphError::NoAllocations) => {
                    Err(Error::SubgraphNotFound(anyhow!("no allocations",)))
                }
                Err(SubgraphError::NoValidVersions) => {
                    Err(Error::SubgraphNotFound(anyhow!("no valid versions",)))
                }
                Ok(None) => Err(Error::SubgraphNotFound(anyhow!("{selector}",))),
                Ok(Some(info)) if info.indexings.is_empty() => Err(Error::NoIndexers),
                Ok(Some(info)) => Ok(Ok(info)),
            }
        }
        QuerySelector::Deployment(ref id) => {
            // Authorization is based on the "authorized subgraphs" allowlist. We need to resolve
            // the subgraph deployments to check if any of the deployment's subgraphs are authorized
            match ctx.network.resolve_with_deployment_id(id) {
                Err(DeploymentError::TransferredToL2) => {
                    Ok(Err(ResolutionError::TransferredToL2 { id_on_l2: None }))
                }
                Err(DeploymentError::NoAllocations) => {
                    Err(Error::SubgraphNotFound(anyhow!("no allocations",)))
                }
                Ok(None) => Err(Error::SubgraphNotFound(anyhow!("{selector}",))),
                Ok(Some(info)) if info.indexings.is_empty() => Err(Error::NoIndexers),
                Ok(Some(info)) => {
                    if !auth.is_any_deployment_subgraph_authorized(&info.subgraphs) {
                        Err(Error::Auth(anyhow!("deployment not authorized by user")))
                    } else {
                        Ok(Ok(info))
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_indexer_queries(
    ctx: Context,
    request_id: String,
    auth: AuthSettings,
    start_time: Instant,
    subgraph: ResolvedSubgraphInfo,
    budget: u128,
    client_request: QueryBody,
    client_response: mpsc::Sender<Result<IndexerResponse, Error>>,
) {
    let one_grt = NotNan::new(1e18).unwrap();
    let grt_per_usd = *ctx.grt_per_usd.borrow();

    // Create the Agora context from the query and variables
    let variables = client_request
        .variables
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    // We handle these errors here, instead of `handle_query`, because the agora context is tied to
    // the lifetime of the query body which may need to extend past the client response. Even if
    // it doesn't, it is relatively difficult to convince the compiler of that.
    let agora_context = match AgoraContext::new(&client_request.query, &variables) {
        Ok(agora_context) => agora_context,
        Err(err) => {
            client_response
                .try_send(Err(Error::BadQuery(anyhow!("{err}"))))
                .unwrap();
            return;
        }
    };

    // Get the chain information for the resolved subgraph
    let chain = ctx.chains.chain(&subgraph.chain);
    let (chain_head, blocks_per_minute, block_requirements) = {
        let chain_reader = chain.read();

        // Get the chain head block number. Try to get it from the chain head tracker service, if it
        // is not available, get the largest block number from the resolved indexers' indexing
        // progress, and if that is not available, default to the subgraph start block.
        let chain_head = chain_reader.latest().map(|b| b.number).unwrap_or_else(|| {
            subgraph
                .latest_reported_block()
                .unwrap_or(subgraph.start_block)
        });

        // Get the estimated blocks per minute for the chain
        let blocks_per_minute = chain_reader.blocks_per_minute();

        let block_requirements =
            match resolve_block_requirements(&chain_reader, &agora_context, subgraph.start_block) {
                Ok(block_requirements) => block_requirements,
                Err(err) => {
                    client_response
                        .try_send(Err(Error::BadQuery(anyhow!("{err}"))))
                        .unwrap();
                    return;
                }
            };

        (chain_head, blocks_per_minute, block_requirements)
    };
    tracing::debug!(chain_head, blocks_per_minute, ?block_requirements);

    let mut indexer_errors = IndexerErrors::default();

    // Candidate selection preparation
    let (mut candidates, errors) = build_candidates_list(
        &ctx,
        &agora_context,
        budget,
        chain_head,
        blocks_per_minute,
        &block_requirements,
        &subgraph.versions,
        subgraph.indexings,
    );
    indexer_errors.extend(errors);

    if tracing::enabled!(tracing::Level::TRACE) {
        tracing::trace!(client_query = client_request.query, variables);
        tracing::trace!(?candidates);
    } else if tracing::enabled!(tracing::Level::DEBUG) && thread_rng().gen_bool(0.01) {
        // Log candidates at a low rate to avoid log bloat
        tracing::debug!(client_query = client_request.query, variables);
        tracing::debug!(?candidates);
    }

    let mut indexer_requests: Vec<reports::IndexerRequest> = Default::default();
    let mut indexer_request_rewrites: BTreeMap<u32, String> = Default::default();
    let mut client_response_time: Option<Duration> = None;

    // If a client query cannot be handled by the available indexers, we should give a reason for
    // all the available indexers in the `bad indexers` response.
    while !candidates.is_empty() && (start_time.elapsed() < Duration::from_secs(60)) {
        let selections: ArrayVec<_, SELECTION_LIMIT> = indexer_selection::select(&candidates);
        if selections.is_empty() {
            // Candidates that would never be selected should be filtered out for improved errors.
            tracing::error!("no candidates selected");
            break;
        }

        let (tx, mut rx) = mpsc::channel(SELECTION_LIMIT);
        let min_fee = *ctx.budgeter.min_indexer_fees.borrow();
        for &selection in &selections {
            let indexer = selection.id;
            let deployment = selection.data.deployment;
            let largest_allocation = selection.data.largest_allocation;
            let url = selection.data.url.clone();
            let seconds_behind = selection.seconds_behind;
            let legacy_scalar = !selection.data.tap_support;
            let subgraph_chain = subgraph.chain.clone();

            // over-pay indexers to hit target
            let min_fee = *(min_fee.0 * grt_per_usd * one_grt) / selections.len() as f64;
            let indexer_fee = selection.fee.as_f64() * budget as f64;
            let fee = indexer_fee.max(min_fee) as u128;
            let receipt = match if legacy_scalar {
                ctx.receipt_signer
                    .create_legacy_receipt(largest_allocation, fee)
            } else {
                ctx.receipt_signer.create_receipt(largest_allocation, fee)
            } {
                Ok(receipt) => receipt,
                Err(err) => {
                    tracing::error!(%indexer, %deployment, error=?err, "failed to create receipt");
                    continue;
                }
            };
            debug_assert!(fee == receipt.grt_value());

            let blocks_behind = blocks_behind(seconds_behind, blocks_per_minute);
            let indexer_query = match indexer_request_rewrites.get(&seconds_behind) {
                Some(indexer_query) => indexer_query.clone(),
                None => {
                    let chain = chain.read();
                    let indexer_query =
                        rewrite_query(&chain, &agora_context, &block_requirements, blocks_behind);
                    if selections
                        .iter()
                        .filter(|s| s.seconds_behind == seconds_behind)
                        .count()
                        > 1
                    {
                        indexer_request_rewrites.insert(seconds_behind, indexer_query.clone());
                    }
                    indexer_query
                }
            };
            let indexer_client = ctx.indexer_client.clone();
            let tx = tx.clone();
            tokio::spawn(
                async move {
                    let start_time = Instant::now();
                    let result = indexer_client
                        .query_indexer(
                            &deployment,
                            &url,
                            &receipt,
                            ctx.attestation_domain,
                            &indexer_query,
                        )
                        .in_current_span()
                        .await;
                    let response_time_ms = start_time.elapsed().as_millis() as u16;
                    let report = reports::IndexerRequest {
                        indexer,
                        deployment,
                        largest_allocation,
                        url: url.to_string(),
                        receipt,
                        subgraph_chain,
                        result,
                        response_time_ms,
                        seconds_behind,
                        blocks_behind,
                        request: indexer_query,
                    };
                    tx.try_send(report).unwrap();
                }
                .instrument(info_span!("indexer_request", ?indexer)),
            );
        }
        drop(tx);

        while let Some(report) = rx.recv().await {
            match report.result.as_ref() {
                Ok(response) if client_response_time.is_none() => {
                    let _ = client_response.try_send(Ok(response.clone()));
                    client_response_time = Some(start_time.elapsed());
                }
                Ok(_) => (),
                Err(err) => {
                    indexer_errors.insert(report.indexer, err.clone());
                }
            }

            let receipt_status = match &report.result {
                Ok(_) => ReceiptStatus::Success,
                Err(IndexerError::Timeout) => ReceiptStatus::Unknown,
                Err(_) => ReceiptStatus::Failure,
            };
            ctx.receipt_signer.record_receipt(
                &report.largest_allocation,
                &report.receipt,
                receipt_status,
            );

            indexer_requests.push(report);
        }

        if client_response_time.is_some() {
            break;
        }

        let selected_indexers: ArrayVec<IndexerId, SELECTION_LIMIT> =
            selections.into_iter().map(|s| s.id).collect();
        candidates.retain(|c| !selected_indexers.contains(&c.id));
    }
    tracing::info!(?indexer_errors);

    let client_response_time = match client_response_time {
        Some(client_response_time) => client_response_time,
        // Send fallback error to use when no indexers are successful.
        None => {
            let _ = client_response.try_send(Err(Error::BadIndexers(indexer_errors.clone())));
            start_time.elapsed()
        }
    };

    let result = if indexer_requests.iter().any(|r| r.result.is_ok()) {
        Ok(())
    } else {
        Err(Error::BadIndexers(indexer_errors))
    };

    let total_fees_grt: f64 = indexer_requests
        .iter()
        .map(|i| i.receipt.grt_value() as f64 * 1e-18)
        .sum();
    let total_fees_usd = USD(NotNan::new(total_fees_grt / *grt_per_usd).unwrap());
    let _ = ctx.budgeter.feedback.send(total_fees_usd);

    for indexer_request in &indexer_requests {
        let latest_block = match &indexer_request.result {
            Ok(response) => response.probe_block.as_ref().map(|b| b.number),
            Err(IndexerError::Unavailable(UnavailableReason::MissingBlock(err))) => err.latest,
            _ => None,
        };
        ctx.indexing_perf.feedback(
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
            allocation = ?indexer_request.receipt.allocation(),
            url = indexer_request.url,
            result = ?indexer_request.result.as_ref().map(|_| ()),
            response_time_ms = indexer_request.response_time_ms,
            seconds_behind = indexer_request.seconds_behind,
            fee = indexer_request.receipt.grt_value() as f64 * 1e-18,
            "indexer_request"
        );
        tracing::trace!(indexer_request = indexer_request.request);
    }

    let response_time_ms = client_response_time.as_millis() as u16;
    let ideal_response_time_ms = indexer_requests
        .iter()
        .filter(|i| i.result.is_ok())
        .map(|i| i.response_time_ms)
        .min()
        .unwrap_or(response_time_ms);
    tracing::info!(
        result = ?result,
        response_time_ms,
        internal_latency_ms = response_time_ms.saturating_sub(ideal_response_time_ms),
        total_fees_grt,
        total_fees_usd = *total_fees_usd.0,
    );

    let _ = ctx.reporter.send(reports::ClientRequest {
        id: request_id,
        response_time_ms,
        result,
        api_key: auth.key,
        user_address: auth.user,
        grt_per_usd,
        indexer_requests,
    });
}

#[derive(CustomDebug)]
struct CandidateMetadata {
    deployment: DeploymentId,
    #[debug(with = std::fmt::Display::fmt)]
    url: Url,
    largest_allocation: AllocationId,
    tap_support: bool,
}

/// Given a list of indexings, build a list of candidates that are within the required block range
/// and have the required performance.
#[allow(clippy::too_many_arguments)]
fn build_candidates_list(
    ctx: &Context,
    context: &AgoraContext,
    budget: u128,
    chain_head: BlockNumber,
    blocks_per_minute: u64,
    block_requirements: &BlockRequirements,
    subgraph_versions: &[DeploymentId],
    indexings: HashMap<IndexingId, Result<Indexing, network::ResolutionError>>,
) -> (
    Vec<Candidate<IndexerId, CandidateMetadata>>,
    BTreeMap<IndexerId, IndexerError>,
) {
    let mut candidates_list = Vec::new();
    let mut candidates_errors = BTreeMap::default();

    // Select the latest subgraph version where indexers are near chain head, or else the latest.
    let cutoff = chain_head.saturating_sub(blocks_per_minute * 30);
    let deployment = *subgraph_versions
        .iter()
        .find(|v| {
            indexings
                .iter()
                .filter_map(|(_, result)| result.as_ref().ok())
                .any(|i| (i.id.deployment == **v) && (i.progress.latest_block > cutoff))
        })
        .unwrap_or(&subgraph_versions[0]);

    // Lock the indexing performance and get access to the latest performance snapshots
    let perf_snapshots = ctx.indexing_perf.latest();

    for (indexing_id, indexing) in indexings {
        // If the indexer is not available, register an error and continue to the next indexer
        let indexing = match indexing {
            Ok(indexing) => indexing,
            Err(err) => {
                candidates_errors.insert(indexing_id.indexer, err.into());
                continue;
            }
        };

        if indexing_id.deployment != deployment {
            continue;
        }

        // Get the performance snapshot for the indexer and calculate the expected performance.
        // If the indexer is not available, register an error and continue to the next indexer
        let perf = match perf_snapshots
            .get(&(indexing_id.indexer, indexing_id.deployment))
            .and_then(|snapshot| perf(snapshot, block_requirements, chain_head, blocks_per_minute))
        {
            Some(perf) => perf,
            None => {
                candidates_errors.insert(
                    indexing_id.indexer,
                    IndexerError::Unavailable(UnavailableReason::Internal(
                        "no indexer performance info",
                    )),
                );
                continue;
            }
        };

        // Check if the indexer is within the required block range
        if let Some((min_block, max_block)) = &block_requirements.range {
            // Allow indexers if their last reported block is "close enough" to the required block
            // range. This is to compensate for the gateway's lack of knowledge about which blocks
            // indexers have responded with already. All else being equal, indexers closer to chain head
            // and with higher success rate will be favored.

            // Infer the indexed range from the indexing progress information
            let range = {
                let (start, end) = indexing.progress.as_range();
                start.unwrap_or(0)..=max(end, perf.latest_block + blocks_per_minute)
            };

            let number_gte = block_requirements.number_gte.unwrap_or(0);

            // If the indexing is not within the required block range, register an error and
            // continue to the next indexer
            let missing_block = match range {
                range if !range.contains(min_block) => Some(*min_block),
                range if !range.contains(max_block) => Some(*max_block),
                range if *range.end() < number_gte => Some(number_gte),
                _ => None,
            };

            if let Some(missing) = missing_block {
                candidates_errors.insert(
                    indexing_id.indexer,
                    IndexerError::Unavailable(UnavailableReason::MissingBlock(MissingBlockError {
                        missing: Some(missing),
                        latest: None,
                    })),
                );
                continue;
            }
        }

        // Calculate the fee for the indexing, and normalize it
        let fee = match indexer_fee(context, &indexing.cost_model) {
            Some(fee) => Normalized::new(fee as f64 / budget as f64).unwrap_or(Normalized::ONE),
            None => {
                candidates_errors.insert(
                    indexing_id.indexer,
                    IndexerError::Unavailable(UnavailableReason::NoFee),
                );
                continue;
            }
        };

        candidates_list.push(Candidate {
            id: indexing_id.indexer,
            data: CandidateMetadata {
                deployment,
                url: indexing.indexer.url.clone(),
                largest_allocation: indexing.largest_allocation,
                tap_support: indexing.indexer.tap_support,
            },
            perf: perf.response,
            fee,
            seconds_behind: perf.seconds_behind,
            slashable_grt: (indexing.indexer.staked_tokens as f64 * 1e-18) as u64,
            zero_allocation: indexing.total_allocated_tokens == 0,
        });
    }

    let seconds_behind_cutoff = 60 * 30;
    if block_requirements.latest
        && candidates_list
            .iter()
            .any(|c| c.seconds_behind <= seconds_behind_cutoff)
    {
        candidates_list.retain(|c| {
            if c.seconds_behind > seconds_behind_cutoff {
                candidates_errors.insert(
                    c.id,
                    IndexerError::Unavailable(UnavailableReason::TooFarBehind),
                );
                return false;
            }
            true
        });
    }

    (candidates_list, candidates_errors)
}

struct Perf {
    response: indexer_selection::ExpectedPerformance,
    latest_block: BlockNumber,
    seconds_behind: u32,
}

fn perf(
    snapshot: &indexing_performance::Snapshot,
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

    let mut response = snapshot.response.expected_performance();
    // Since our gateway is specialized for frontends, add an additional penalty for candidates
    // far behind chain head. This compensates for the impacts of information decay and the sharp
    // dropoff of our `seconds_behind` curve.
    if seconds_behind > (60 * 30) {
        response.success_rate = Normalized::ZERO;
    }

    Some(Perf {
        response,
        latest_block,
        seconds_behind,
    })
}

fn blocks_behind(seconds_behind: u32, blocks_per_minute: u64) -> u64 {
    ((seconds_behind as f64 / 60.0) * blocks_per_minute as f64) as u64
}

/// Estimate the fee for an indexer based on the cost model and the query context.
///
/// If the cost model is not available, the fee is assumed to be zero.
/// If the cost model is available but the cost cannot be calculated, the fee is assumed to be
/// unavailable.
pub fn indexer_fee(context: &AgoraContext, cost_model: &Option<Ptr<CostModel>>) -> Option<u128> {
    match cost_model
        .as_ref()
        .map(|model| model.cost_with_context(context))
    {
        None => Some(0),
        Some(Ok(fee)) => fee.to_u128(),
        Some(Err(_)) => None,
    }
}

impl From<network::ResolutionError> for IndexerError {
    fn from(err: network::ResolutionError) -> Self {
        match err {
            network::ResolutionError::Unavailable(reason) => {
                let reason = match reason {
                    network::UnavailableReason::Blocked => UnavailableReason::Blocked,
                    network::UnavailableReason::BlockedBadPOI => UnavailableReason::BlockedBadPOI,
                    reason @ network::UnavailableReason::IndexerServiceVersionBelowMin
                    | reason @ network::UnavailableReason::GraphNodeVersionBelowMin => {
                        UnavailableReason::NotSupported(reason.to_string())
                    }
                    reason @ network::UnavailableReason::IndexerResolutionError { .. }
                    | reason @ network::UnavailableReason::IndexingProgressNotFound => {
                        UnavailableReason::NoStatus(reason.to_string())
                    }
                };
                IndexerError::Unavailable(reason)
            }
            network::ResolutionError::Internal(err) => {
                tracing::error!(error = ?err, "internal error");
                IndexerError::Unavailable(UnavailableReason::Internal(err))
            }
        }
    }
}

pub async fn handle_indexer_query(
    State(ctx): State<Context>,
    Extension(auth): Extension<AuthSettings>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    _query_settings: Option<Extension<QuerySettings>>,
    Path((deployment, indexer)): Path<(DeploymentId, IndexerId)>,
    payload: String,
) -> Result<Response<String>, Error> {
    let start_time = Instant::now();

    let bad_indexers = |err: IndexerError| -> Error {
        Error::BadIndexers(IndexerErrors::from_iter([(indexer, err)]))
    };

    let indexing_id = IndexingId {
        deployment,
        indexer,
    };
    let subgraph = resolve_subgraph_info(&ctx, &auth, QuerySelector::Deployment(deployment))
        .await?
        .map_err(|err| match err {
            ResolutionError::TransferredToL2 { .. } => {
                Error::SubgraphNotFound(anyhow!("deployment transferred to L2"))
            }
        })?;
    let indexing = subgraph
        .indexings
        .get(&indexing_id)
        .ok_or_else(|| Error::NoIndexers)?
        .as_ref()
        .map_err(|err| bad_indexers(err.clone().into()))?;

    let (latest_block, blocks_per_minute) = {
        let chain = ctx.chains.chain(&subgraph.chain);
        let chain = chain.read();
        let latest_block = chain.latest().map(|block| block.number);
        (latest_block, chain.blocks_per_minute())
    };
    let blocks_behind = latest_block
        .map(|head| head.saturating_sub(indexing.progress.latest_block))
        .unwrap_or(0);
    let seconds_behind = ((blocks_behind as f64 * 60.0) / blocks_per_minute as f64) as u32;

    // Use budget as fee.
    let grt_per_usd = *ctx.grt_per_usd.borrow();
    let one_grt = NotNan::new(1e18).unwrap();
    let fee = *(ctx.budgeter.query_fees_target.0 * grt_per_usd * one_grt) as u128;

    let allocation = indexing.largest_allocation;
    let receipt = match if indexing.indexer.tap_support {
        ctx.receipt_signer.create_receipt(allocation, fee)
    } else {
        ctx.receipt_signer.create_legacy_receipt(allocation, fee)
    } {
        Ok(receipt) => receipt,
        Err(err) => {
            return Err(Error::Internal(anyhow!("failed to create receipt: {err}")));
        }
    };

    let indexer_start_time = Instant::now();
    let result = ctx
        .indexer_client
        .query_indexer(
            &deployment,
            &indexing.indexer.url,
            &receipt,
            ctx.attestation_domain,
            &payload,
        )
        .in_current_span()
        .await;
    let response_time_ms = start_time.elapsed().as_millis() as u16;
    let indexer_request = reports::IndexerRequest {
        indexer: indexing_id.indexer,
        deployment: indexing_id.deployment,
        largest_allocation: allocation,
        url: indexing.indexer.url.to_string(),
        receipt,
        subgraph_chain: subgraph.chain,
        result: result.clone(),
        response_time_ms: indexer_start_time.elapsed().as_millis() as u16,
        seconds_behind,
        blocks_behind,
        request: payload,
    };

    let report_result = match &result {
        Ok(_) => Ok(()),
        Err(err) => Err(bad_indexers(err.clone())),
    };
    let result = result.map_err(bad_indexers);

    let deployment = indexing_id.deployment.to_string();
    let indexer = indexing_id.indexer.to_string();
    let labels = [deployment.as_str(), indexer.as_str()];
    METRICS
        .indexer_query
        .check(&labels, &indexer_request.result);
    with_metric(&METRICS.indexer_query.duration, &labels, |hist| {
        hist.observe(indexer_request.response_time_ms as f64)
    });
    match &result {
        Ok(_) => METRICS.client_query.ok.inc(),
        Err(_) => METRICS.client_query.err.inc(),
    };
    METRICS
        .client_query
        .duration
        .observe(response_time_ms as f64);

    tracing::info!(
        indexer = ?indexer_request.indexer,
        deployment = %indexer_request.deployment,
        allocation = ?indexer_request.receipt.allocation(),
        url = indexer_request.url,
        result = ?indexer_request.result.as_ref().map(|_| ()),
        response_time_ms = indexer_request.response_time_ms,
        seconds_behind = indexer_request.seconds_behind,
        fee = indexer_request.receipt.grt_value() as f64 * 1e-18,
        "indexer_request"
    );

    ctx.indexing_perf.feedback(
        indexer_request.indexer,
        indexer_request.deployment,
        indexer_request.result.is_ok(),
        indexer_request.response_time_ms,
        latest_block,
    );

    let _ = ctx.reporter.send(reports::ClientRequest {
        id: request_id,
        response_time_ms,
        result: report_result,
        api_key: auth.key,
        user_address: auth.user,
        grt_per_usd,
        indexer_requests: vec![indexer_request],
    });

    result.map(
        |IndexerResponse {
             client_response,
             attestation,
             ..
         }| {
            Response::builder()
                .status(StatusCode::OK)
                .header_typed(ContentType::json())
                .header_typed(GraphAttestation(attestation))
                .body(client_response)
                .unwrap()
        },
    )
}

#[cfg(test)]
mod tests {
    mod require_req_auth {
        use std::collections::HashMap;

        use assert_matches::assert_matches;
        use axum::{
            body::Body,
            http::{Method, Request, StatusCode},
            middleware,
            routing::post,
            Extension, Router,
        };
        use gateway_framework::{
            auth::{api_keys::APIKey, AuthContext, AuthSettings},
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
            };
            if let Some(key) = key {
                ctx.api_keys = watch::channel(HashMap::from([(
                    key.into(),
                    APIKey {
                        key: key.into(),
                        ..Default::default()
                    },
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
            async fn handler(Extension(auth): Extension<AuthSettings>) -> String {
                auth.key
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
                assert_eq!(res_body.errors[0].message, "auth error: API key not found");
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
