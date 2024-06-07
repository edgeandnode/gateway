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
    http::{HeaderMap, Response, StatusCode},
    Extension,
};
use cost_model::{Context as AgoraContext, CostModel};
use eventuals::Ptr;
use gateway_common::utils::http_ext::HttpBuilderExt as _;
use gateway_framework::{
    auth::AuthSettings,
    budgets::USD,
    errors::{
        Error, IndexerError,
        UnavailableReason::{self, MissingBlock},
    },
    gateway::http::{
        gateway::{DeterministicRequest, IndexerResponse},
        requests::{
            auth::resolve_and_authorize_deployments, blocks::resolve_block_requirements,
            budget::Budget, candidates::prepare_candidate, selector::RequestSelector,
        },
    },
    http::middleware::RequestId,
    indexing::Indexing,
    metrics::{with_metric, METRICS},
    reports,
    topology::network::{Deployment, Manifest},
};
use headers::ContentType;
use indexer_selection::{ArrayVec, Candidate};
use num_traits::cast::ToPrimitive as _;
use ordered_float::NotNan;
use prost::bytes::Buf;
use rand::{thread_rng, Rng as _};
use serde::Deserialize;
use serde_json::value::RawValue;
use thegraph_core::types::DeploymentId;
use tokio::sync::mpsc;
use tracing::Instrument as _;

use self::{
    attestation_header::GraphAttestation, context::Context, l2_forwarding::forward_request_to_l2,
};
use crate::block_constraints::{block_constraints, rewrite_query};

mod attestation_header;
pub mod context;
mod l2_forwarding;

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
    OriginalUri(original_uri): OriginalUri,
    selector: RequestSelector,
    headers: HeaderMap,
    payload: Bytes,
) -> Result<Response<String>, Error> {
    let start_time = Instant::now();

    let (deployments, subgraph) =
        resolve_and_authorize_deployments(&ctx.network, &auth, &selector)?;

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
        return Err(Error::NoIndexers);
    }

    let manifest = deployments
        .last()
        .map(|deployment| deployment.manifest.clone())
        .ok_or_else(|| Error::SubgraphNotFound(anyhow!("no matching deployments")))?;

    let client_request: QueryBody =
        serde_json::from_reader(payload.reader()).map_err(|err| Error::BadQuery(err.into()))?;

    let budget = Budget::calculate(&ctx.grt_per_usd, &ctx.budgeter, &auth)?;

    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(
        run_indexer_queries(
            ctx,
            request_id,
            auth,
            start_time,
            deployments,
            available_indexers,
            manifest,
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
        .observe(Instant::now().duration_since(start_time).as_secs_f64());

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

#[allow(clippy::too_many_arguments)]
async fn run_indexer_queries(
    ctx: Context,
    request_id: String,
    auth: AuthSettings,
    start_time: Instant,
    deployments: Vec<Arc<Deployment>>,
    mut available_indexers: BTreeSet<Indexing>,
    manifest: Manifest,
    budget: Budget,
    client_request: QueryBody,
    client_response: mpsc::Sender<Result<IndexerResponse, Error>>,
) {
    let one_grt = budget.one_grt;
    let grt_per_usd = budget.grt_per_usd;

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
    let chain = ctx.chains.chain(&manifest.network).await;
    let chain_reader = chain.read().await;
    let blocks_per_minute = chain_reader.blocks_per_minute();
    let chain_head = chain_reader.latest().map(|b| b.number);
    let block_constraints = block_constraints(&agora_context).unwrap_or_default();
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

    let indexing_statuses = ctx.indexing_statuses.value_immediate().unwrap();
    let chain_head = chain_head.unwrap_or_else(|| {
        available_indexers
            .iter()
            .flat_map(|indexing| indexing_statuses.get(indexing).map(|status| status.block))
            .max()
            .unwrap_or(0) // doesn't matter if no indexers have status
    });
    tracing::debug!(chain_head, blocks_per_minute, ?block_requirements);

    let mut indexer_errors: BTreeMap<Address, IndexerError> = Default::default();
    let blocklist = ctx
        .indexings_blocklist
        .value_immediate()
        .unwrap_or_default();
    available_indexers.retain(|candidate| {
        if blocklist.contains(candidate) || ctx.bad_indexers.contains(&candidate.indexer) {
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
        let perf = ctx.indexing_perf.latest();
        for indexing in available_indexers {
            if let Some(status) = indexing_statuses.get(&indexing) {
                // If the indexer status indicates it supports Scalar TAP, add it to the set of
                // indexers with Scalar TAP support.
                if !status.legacy_scalar {
                    indexers_with_tap_support.insert(indexing.indexer);
                }
            }

            match prepare_candidate(
                &ctx.network,
                |request, status| indexer_fee(&status.cost_model, request),
                &indexing_statuses,
                &perf,
                &versions_behind,
                &agora_context,
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
        tracing::trace!(client_query = client_request.query, variables);
        tracing::trace!(?candidates);
    } else if tracing::enabled!(tracing::Level::DEBUG) && thread_rng().gen_bool(0.01) {
        tracing::debug!(client_query = client_request.query, variables);
        tracing::debug!(?candidates);
    }

    let mut indexer_requests: Vec<reports::IndexerRequest> = Default::default();
    let mut indexer_request_rewrites: BTreeMap<u32, String> = Default::default();
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
        let min_fee = *ctx.budgeter.min_indexer_fees.borrow();
        for &selection in &selections {
            let indexer = selection.indexer;
            let deployment = selection.deployment;
            let url = selection.url.clone();
            let seconds_behind = selection.seconds_behind;
            let legacy_scalar = !indexers_with_tap_support.contains(&indexer);
            let subgraph_chain = manifest.network.clone();

            // over-pay indexers to hit target
            let min_fee = *(min_fee.0 * grt_per_usd * one_grt) / selections.len() as f64;
            let indexer_fee = selection.fee.as_f64() * budget.budget as f64;
            let fee = indexer_fee.max(min_fee) as u128;
            let receipt = match if legacy_scalar {
                ctx.receipt_signer
                    .create_legacy_receipt(indexer, deployment, fee)
                    .await
            } else {
                ctx.receipt_signer
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
            let indexer_query = match indexer_request_rewrites.get(&seconds_behind) {
                Some(indexer_query) => indexer_query.clone(),
                None => {
                    let chain = chain.read().await;
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
            tokio::spawn(async move {
                let start_time = Instant::now();
                let result = indexer_client
                    .query_indexer(
                        &deployment,
                        &url,
                        &receipt,
                        ctx.attestation_domain,
                        &indexer_query,
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
                    request: DeterministicRequest {
                        body: indexer_query,
                        headers: Default::default(),
                    },
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
    let total_fees_usd = USD(NotNan::new(total_fees_grt / *grt_per_usd).unwrap());
    let _ = ctx.budgeter.feedback.send(total_fees_usd);

    for indexer_request in &indexer_requests {
        let latest_block = match &indexer_request.result {
            Ok(response) => response.probe_block.as_ref().map(|b| b.number),
            Err(IndexerError::Unavailable(MissingBlock(err))) => err.latest,
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

pub fn indexer_fee(
    cost_model: &Option<Ptr<CostModel>>,
    context: &AgoraContext,
) -> Result<u128, IndexerError> {
    match cost_model
        .as_ref()
        .map(|model| model.cost_with_context(context))
    {
        None => Ok(0),
        Some(Ok(fee)) => fee
            .to_u128()
            .ok_or(IndexerError::Unavailable(UnavailableReason::NoFee)),
        Some(Err(_)) => Err(IndexerError::Unavailable(UnavailableReason::NoFee)),
    }
}

fn blocks_behind(seconds_behind: u32, blocks_per_minute: u64) -> u64 {
    ((seconds_behind as f64 / 60.0) * blocks_per_minute as f64) as u64
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
