mod auth;
mod block_constraints;
mod blocks;
mod budgets;
mod bytes;
mod chain;
mod chains;
mod client_query;
mod config;
mod errors;
mod exchange_rate;
mod graphql;
mod indexer_client;
mod indexing_performance;
mod metrics;
mod middleware;
mod network;
mod receipts;
mod reports;
mod subgraph_studio;
mod time;
mod unattestable_errors;

use std::{
    collections::HashSet,
    env,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use auth::AuthContext;
use axum::{
    http::{self, status::StatusCode},
    routing, Router,
};
use budgets::{Budgeter, USD};
use chains::Chains;
use client_query::context::Context;
use config::{ApiKeys, BlocklistEntry, ExchangeRateProvider};
use indexer_client::IndexerClient;
use indexing_performance::IndexingPerformance;
use middleware::{
    legacy_auth_adapter, RequestTracingLayer, RequireAuthorizationLayer, SetRequestIdLayer,
};
use network::{indexer_blocklist, subgraph_client::Client as SubgraphClient};
use prometheus::{self, Encoder as _};
use receipts::ReceiptSigner;
use thegraph_core::{
    alloy::{dyn_abi::Eip712Domain, primitives::ChainId, signers::local::PrivateKeySigner},
    attestation,
};
use tokio::{net::TcpListener, signal::unix::SignalKind, sync::watch};
use tower_http::cors::{self, CorsLayer};
use tracing_subscriber::{prelude::*, EnvFilter};

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

    let receipt_signer = PrivateKeySigner::from_bytes(&conf.receipts.signer)
        .expect("failed to prepare receipt signer");
    let signer_address = receipt_signer.address();

    init_logging("graph-gateway", conf.log_json);
    tracing::info!("gateway ID: {:?}", signer_address);

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let grt_per_usd = match conf.exchange_rate_provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => watch::channel(grt_per_usd).1,
        ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await,
    };

    let attestation_domain: &'static Eip712Domain =
        Box::leak(Box::new(attestation::eip712_domain(
            conf.attestations
                .chain_id
                .parse::<ChainId>()
                .expect("failed to parse attestation domain chain_id"),
            conf.attestations.dispute_manager,
        )));

    let indexer_client = IndexerClient {
        client: http_client.clone(),
    };
    let network_subgraph_client = SubgraphClient {
        client: indexer_client.clone(),
        indexers: conf.trusted_indexers,
        latest_block: None,
        page_size: 500,
    };
    let indexer_host_blocklist = match &conf.ip_blocker_db {
        Some(path) => {
            config::load_ip_blocklist_from_file(path).expect("failed to load IP blocker DB")
        }
        None => Default::default(),
    };
    let indexer_blocklist = indexer_blocklist::Blocklist::spawn(conf.blocklist);
    let mut network = network::service::spawn(
        http_client.clone(),
        network_subgraph_client,
        indexer_blocklist.clone(),
        conf.min_indexer_version,
        conf.min_graph_node_version,
        indexer_host_blocklist,
    );
    let indexing_perf = IndexingPerformance::new(network.clone());
    network.wait_until_ready().await;

    let receipt_signer: &'static ReceiptSigner = Box::leak(Box::new(ReceiptSigner::new(
        receipt_signer,
        conf.receipts.chain_id,
        conf.receipts.verifier,
    )));

    // Initialize the auth service
    let auth_service =
        init_auth_service(http_client.clone(), conf.api_keys, conf.payment_required).await;

    let budgeter: &'static Budgeter =
        Box::leak(Box::new(Budgeter::new(USD(conf.query_fees_target))));

    let reporter = reports::Reporter::create(
        signer_address,
        conf.graph_env_id,
        reports::Topics {
            queries: "gateway_queries",
            attestations: "gateway_attestations",
        },
        conf.kafka,
    )
    .unwrap();

    let ctx = Context {
        indexer_client,
        receipt_signer,
        budgeter,
        chains: Box::leak(Box::new(Chains::new(conf.chain_aliases))),
        grt_per_usd,
        indexing_perf,
        network,
        attestation_domain,
        reporter,
    };

    let blocklist: watch::Receiver<Vec<BlocklistEntry>> = indexer_blocklist.blocklist;

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

    let api = Router::new()
        .route(
            "/deployments/id/:deployment_id",
            routing::post(client_query::handle_query),
        )
        .route(
            "/deployments/id/:deployment_id/indexers/id/:indexer",
            routing::post(client_query::handle_indexer_query),
        )
        .route(
            "/subgraphs/id/:subgraph_id",
            routing::post(client_query::handle_query),
        )
        .route(
            "/:api_key/deployments/id/:deployment_id",
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
                .layer(SetRequestIdLayer::new(format!("{:?}", signer_address)))
                // Handle legacy in-path auth, and convert it into a header
                .layer(axum::middleware::from_fn(legacy_auth_adapter))
                // Require the query to be authorized
                .layer(RequireAuthorizationLayer::new(auth_service)),
        );

    let router = Router::new()
        .route("/", routing::get(|| async { "Ready to roll!" }))
        // This path is required by NGINX ingress controller
        .route("/ready", routing::get(|| async { "Ready" }))
        .route(
            "/blocklist",
            routing::get(move || async move { axum::Json(blocklist.borrow().clone()) }),
        )
        .nest("/api", api);

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

pub fn init_logging(executable_name: &str, json: bool) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::try_new(format!("info,{executable_name}=debug")).unwrap());

    let log_default_layer = (!json).then(tracing_subscriber::fmt::layer);
    let log_json_layer = json.then(|| {
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
    });

    tracing_subscriber::registry()
        .with(env_filter)
        .with(log_default_layer)
        .with(log_json_layer)
        .init();
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
            subgraph_studio::api_keys(http, url, auth).await
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
