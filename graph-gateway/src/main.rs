use std::{
    collections::HashSet,
    env,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{Address, U256};
use alloy_sol_types::Eip712Domain;
use axum::{
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, status::StatusCode, Request},
    middleware,
    middleware::Next,
    response::Response,
    routing, Router,
};
use config::{ApiKeys, ExchangeRateProvider};
use gateway_framework::{
    auth::AuthContext,
    budgets::{Budgeter, USD},
    chains::Chains,
    exchange_rate,
    http::middleware::{
        legacy_auth_adapter, RequestTracingLayer, RequireAuthorizationLayer, SetRequestIdLayer,
    },
    json, logging,
};
use graph_gateway::{
    client_query::{self, context::Context},
    indexer_client::IndexerClient,
    indexing_performance::IndexingPerformance,
    network::{
        subgraph_client::Client as NetworkSubgraphClient, NetworkService, NetworkServiceBuilder,
    },
    receipts::ReceiptSigner,
    reports, subgraph_studio, vouchers,
};
use prometheus::{self, Encoder as _};
use secp256k1::SecretKey;
use semver::Version;
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use thegraph_core::{client::Client as SubgraphClient, types::attestation};
use tokio::{
    net::TcpListener,
    signal::unix::SignalKind,
    sync::watch,
    time::{interval, MissedTickBehavior},
};
use tower_http::cors::{self, CorsLayer};
use uuid::Uuid;

mod config;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    let conf_path = env::args()
        .nth(1)
        .expect("Missing argument for config path")
        .parse::<PathBuf>()
        .unwrap();
    let conf = config::load_from_file(&conf_path).expect("Failed to load config");

    // Get the gateway ID from the config or generate a new one.
    let gateway_id = conf
        .gateway_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let conf_repr = format!("{conf:?}");

    logging::init("graph-gateway", conf.log_json);
    tracing::info!("gateway ID: {}", gateway_id);
    tracing::debug!(config = %conf_repr);

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let grt_per_usd = match conf.exchange_rate_provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => watch::channel(grt_per_usd).1,
        ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await.unwrap(),
    };

    let attestation_domain: &'static Eip712Domain =
        Box::leak(Box::new(attestation::eip712_domain(
            U256::from_str_radix(&conf.attestations.chain_id, 10)
                .expect("failed to parse attestation domain chain_id"),
            conf.attestations.dispute_manager,
        )));

    // Initialize the network service and wait for the initial network state synchronization
    let network_subgraph_client =
        SubgraphClient::new(http_client.clone(), conf.network_subgraph.clone());
    let mut network = match init_network_service(
        network_subgraph_client,
        conf.l2_gateway.is_some(),
        http_client.clone(),
        conf.min_indexer_version,
        conf.min_graph_node_version,
        conf.bad_indexers,
        conf.ip_blocker_db.as_deref(),
        conf.poi_blocklist,
    ) {
        Ok(network) => network,
        Err(err) => {
            tracing::error!(%err);
            panic!("Failed to initialize the network service: {err}");
        }
    };
    let indexing_perf = IndexingPerformance::new(network.clone());
    network.wait_until_ready().await;

    let legacy_signer: &'static SecretKey = Box::leak(Box::new(
        conf.scalar
            .legacy_signer
            .map(|s| s.0)
            .unwrap_or(conf.scalar.signer.0),
    ));
    let receipt_signer: &'static ReceiptSigner = Box::leak(Box::new(ReceiptSigner::new(
        conf.scalar.signer.0,
        conf.scalar.chain_id,
        conf.scalar.verifier,
        legacy_signer,
    )));

    // Initialize the auth service
    let auth_service =
        init_auth_service(http_client.clone(), conf.api_keys, conf.payment_required).await;

    let budgeter: &'static Budgeter =
        Box::leak(Box::new(Budgeter::new(USD(conf.query_fees_target))));

    let reporter = reports::Reporter::create(
        conf.graph_env_id,
        conf.query_fees_target,
        "gateway_client_query_results",
        "gateway_indexer_attempts",
        "gateway_attestations",
        conf.kafka,
    )
    .unwrap();

    let ctx = Context {
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        receipt_signer,
        budgeter,
        l2_gateway: conf.l2_gateway,
        chains: Box::leak(Box::new(Chains::new(conf.chain_aliases))),
        grt_per_usd,
        indexing_perf,
        network,
        attestation_domain,
        reporter,
    };

    // Host metrics on a separate server with a port that isn't open to public requests.
    tokio::spawn(async move {
        let router = Router::new().route("/metrics", routing::get(handle_metrics));

        let metrics_listener = TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            conf.port_metrics,
        ))
        .await
        .expect("Failed to bind metrics server");

        axum::serve(metrics_listener, router.into_make_service())
            // disable Nagle's algorithm
            .tcp_nodelay(true)
            .await
            .expect("Failed to start metrics server");
    });

    let rate_limiter_slots = 10;
    let rate_limiter: &'static RateLimiter<String> =
        Box::leak(Box::new(RateLimiter::<String>::new(
            rate_limiter_slots * conf.ip_rate_limit as usize,
            rate_limiter_slots,
        )));
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            rate_limiter.rotate_slots();
        }
    });

    let api = Router::new()
        .route(
            "/deployments/id/:deployment_id",
            routing::post(client_query::handle_query),
        )
        .route(
            "/:api_key/deployments/id/:deployment_id",
            routing::post(client_query::handle_query),
        )
        .route(
            "/subgraphs/id/:subgraph_id",
            routing::post(client_query::handle_query),
        )
        .route(
            "/:api_key/subgraphs/id/:subgraph_id",
            routing::post(client_query::handle_query),
        )
        .with_state(ctx)
        .layer(
            // ServiceBuilder works by composing all layers into one such that they run top to
            // bottom, and then the response would bubble back up through the layers in reverse
            tower::ServiceBuilder::new()
                .layer(
                    CorsLayer::new()
                        .allow_origin(cors::Any)
                        .allow_headers(cors::Any)
                        .allow_methods([http::Method::OPTIONS, http::Method::POST]),
                )
                // Set up the query tracing span
                .layer(RequestTracingLayer)
                // Set the query ID on the request
                .layer(SetRequestIdLayer::new(gateway_id))
                // Handle legacy in-path auth, and convert it into a header
                .layer(middleware::from_fn(legacy_auth_adapter))
                // Require the query to be authorized
                .layer(RequireAuthorizationLayer::new(auth_service)),
        );

    let router = Router::new()
        .route("/", routing::get(|| async { "Ready to roll!" }))
        // This path is required by NGINX ingress controller
        .route("/ready", routing::get(|| async { "Ready" }))
        .route(
            "/collect-receipts",
            routing::post(vouchers::handle_collect_receipts)
                .with_state(legacy_signer)
                .layer(DefaultBodyLimit::max(3_000_000)),
        )
        .route(
            "/partial-voucher",
            routing::post(vouchers::handle_partial_voucher)
                .with_state(legacy_signer)
                .layer(DefaultBodyLimit::max(3_000_000)),
        )
        .route(
            "/voucher",
            routing::post(vouchers::handle_voucher).with_state(legacy_signer),
        )
        .route(
            "/budget",
            routing::get(|| async { budgeter.query_fees_target.0.to_string() }),
        )
        .nest("/api", api)
        .layer(middleware::from_fn_with_state(rate_limiter, ip_rate_limit));

    let app_listener = TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        conf.port_api,
    ))
    .await
    .expect("Failed to bind API server");
    axum::serve(
        app_listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    // disable Nagle's algorithm
    .tcp_nodelay(true)
    .with_graceful_shutdown(await_shutdown_signals())
    .await
    .expect("Failed to start API server");
    tracing::warn!("shutdown");
}

async fn await_shutdown_signals() {
    #[cfg(unix)]
    let sigint = async {
        tokio::signal::unix::signal(SignalKind::interrupt())
            .expect("install SIGINT handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigint = std::future::pending::<()>();

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = sigint => (),
        _ = sigterm => (),
    }
}

async fn ip_rate_limit(
    State(limiter): State<&'static RateLimiter<String>>,
    ConnectInfo(info): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, json::JsonResponse> {
    if limiter.check_limited(info.ip().to_string()) {
        return Err(graphql_error_response("Too many requests, try again later"));
    }

    Ok(next.run(req).await)
}

async fn handle_metrics() -> impl axum::response::IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if let Err(metrics_encode_err) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(%metrics_encode_err);
        buffer.clear();
        write!(&mut buffer, "Failed to encode metrics").unwrap();
        return (StatusCode::INTERNAL_SERVER_ERROR, buffer);
    }
    (StatusCode::OK, buffer)
}

fn graphql_error_response<S: ToString>(message: S) -> json::JsonResponse {
    json::json_response([], json!({"errors": [{"message": message.to_string()}]}))
}

/// Creates a new [`AuthContext`] from the given configuration.
///
/// This functions awaits the completion of the initial API keys fetch.
async fn init_auth_service(
    http: reqwest::Client,
    config: Option<ApiKeys>,
    payment_required: bool,
) -> AuthContext {
    let special_api_keys = match &config {
        Some(ApiKeys::Endpoint { special, .. }) => Arc::new(HashSet::from_iter(special.clone())),
        _ => Default::default(),
    };

    let api_keys = match config {
        Some(ApiKeys::Endpoint { url, auth, .. }) => {
            subgraph_studio::api_keys(http, url, auth.0).await
        }
        Some(ApiKeys::Fixed(api_keys)) => {
            let api_keys = api_keys.into_iter().map(|k| (k.key.clone(), k)).collect();
            watch::channel(api_keys).1
        }
        None => watch::channel(Default::default()).1,
    };

    AuthContext {
        payment_required,
        api_keys,
        special_api_keys,
    }
}

/// Creates a new network service instance based on the provided configuration, spawning the
/// necessary background tasks.
#[allow(clippy::too_many_arguments)]
fn init_network_service(
    subgraph_client: SubgraphClient,
    subgraph_client_l2_transfer_support: bool,
    indexer_http_client: reqwest::Client,
    indexer_min_indexer_service_version: Version,
    indexer_min_graph_node_version: Version,
    indexer_addr_blocklist: Vec<Address>,
    indexer_host_blocklist: Option<&Path>,
    indexer_pois_blocklist: Vec<config::ProofOfIndexingInfo>,
) -> anyhow::Result<NetworkService> {
    let subgraph_client =
        NetworkSubgraphClient::new(subgraph_client, subgraph_client_l2_transfer_support);

    let mut builder = NetworkServiceBuilder::new(subgraph_client, indexer_http_client);

    // Configure the minimum  and graph node versions required by indexers
    builder = builder.with_indexer_min_indexer_service_version(indexer_min_indexer_service_version);
    builder = builder.with_indexer_min_graph_node_version(indexer_min_graph_node_version);

    // Configure the address-based blocklist for indexers
    if !indexer_addr_blocklist.is_empty() {
        let indexer_addr_blocklist = indexer_addr_blocklist.into_iter().collect();
        builder = builder.with_indexer_addr_blocklist(indexer_addr_blocklist);
    }

    // Load and configure the host-based blocklist for indexers
    if let Some(indexer_host_blocklist) = indexer_host_blocklist {
        let indexer_host_blocklist = config::load_ip_blocklist_from_file(indexer_host_blocklist)?;
        builder = builder.with_indexer_host_blocklist(indexer_host_blocklist);
    }

    // Load and configure the POI-based blocklist for indexers
    if !indexer_pois_blocklist.is_empty() {
        let indexer_pois_blocklist = indexer_pois_blocklist.into_iter().map(Into::into).collect();
        builder = builder.with_indexer_pois_blocklist(indexer_pois_blocklist);
    }

    Ok(builder.build().spawn())
}
