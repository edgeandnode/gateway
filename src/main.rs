//! Graph Gateway Entry Point
//!
//! Initializes and starts the graph-gateway service, routing GraphQL queries
//! from clients to Graph Network indexers.
//!
//! # Initialization Sequence
//!
//! 1. Load configuration from JSON file (path from CLI argument)
//! 2. Initialize receipt signer from config and set up logging
//! 3. Set up HTTP client with 20-second timeout
//! 4. Initialize exchange rate provider (RPC or fixed value)
//! 5. Create attestation EIP-712 domains for signature verification
//! 6. Initialize network topology service and wait for initial data
//! 7. Initialize auth service from API keys source (endpoint, Kafka, or fixed)
//! 8. Create query budgeter with PID controller for fee management
//! 9. Start metrics server on separate port
//! 10. Start main API server with CORS and auth middleware
//!
//! # Static Allocations
//!
//! Several components use `Box::leak()` to create `&'static` references:
//!
//! - `attestation_domain` / `legacy_attestation_domain`: EIP-712 domains for attestation
//!   verification. Static because they're immutable config derived from chain ID and
//!   dispute manager address.
//!
//! - `receipt_signer`: TAP receipt signing service. Static because it holds the signing
//!   key and is used by all query handlers.
//!
//! - `budgeter`: Fee budget controller. Static because it maintains state across all
//!   requests and runs a background task.
//!
//! - `chains`: Chain head tracking. Static because it aggregates block info from all
//!   query responses.
//!
//! This pattern is intentional: these are singletons that must outlive Axum's state
//! lifetime requirements and are never deallocated during the gateway's lifetime.
//!
//! # Graceful Shutdown
//!
//! The gateway handles SIGINT and SIGTERM for graceful shutdown. The Axum server
//! stops accepting new connections and waits for in-flight requests to complete.

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
mod kafka;
mod metrics;
mod middleware;
mod network;
mod receipts;
mod reports;
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
    Router,
    http::{self, status::StatusCode},
    routing,
    serve::ListenerExt,
};
use budgets::{Budgeter, USD};
use chains::Chains;
use client_query::context::Context;
use config::{ApiKeys, BlocklistEntry, ExchangeRateProvider};
use indexer_client::IndexerClient;
use indexing_performance::IndexingPerformance;
use middleware::{RequestTracingLayer, RequireAuthorizationLayer, legacy_auth_adapter};
use network::{indexer_blocklist, subgraph_client::Client as SubgraphClient};
use prometheus::{self, Encoder as _};
use rand::RngCore as _;
use receipts::ReceiptSigner;
use thegraph_core::{
    alloy::{dyn_abi::Eip712Domain, primitives::ChainId, signers::local::PrivateKeySigner},
    attestation,
};
use tokio::{net::TcpListener, signal::unix::SignalKind, sync::watch};
use tower_http::cors::{self, CorsLayer};
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::KafkaConfig;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    // Phase 1: Load configuration
    let conf = load_configuration();

    // Phase 2: Initialize signing key and logging
    let receipt_key = PrivateKeySigner::from_bytes(&conf.receipts.signer)
        .expect("failed to prepare receipt signer");
    let signer_address = receipt_key.address();
    init_logging("graph-gateway", conf.log_json);
    tracing::info!("gateway ID: {signer_address:?}");

    // Set up termination handler (will be replaced by graceful shutdown)
    let setup_termination = tokio::spawn(async {
        await_shutdown_signals().await;
        tracing::warn!("shutdown");
        std::process::exit(1);
    });

    // Phase 3: Initialize core services
    let http_client = init_http_client();
    let kafka_consumer = KafkaConsumer::new(conf.kafka.clone());
    let grt_per_usd = init_exchange_rate(&conf.exchange_rate_provider).await;

    // Phase 4: Create attestation domains (static for Axum state lifetime)
    let (attestation_domain, legacy_attestation_domain) =
        init_attestation_domains(&conf.attestations);

    // Phase 5: Initialize network topology service
    let indexer_client = IndexerClient {
        client: http_client.clone(),
    };
    let (mut network, indexer_blocklist, indexing_perf) =
        init_network_service(&http_client, &indexer_client, &conf, kafka_consumer.clone());
    network.wait_until_ready().await;

    // Phase 6: Create receipt signer (static for Axum state lifetime)
    let receipt_signer: &'static ReceiptSigner = Box::leak(Box::new(ReceiptSigner::new(
        conf.receipts.payer,
        receipt_key,
        conf.receipts.chain_id,
        conf.receipts.verifier,
        conf.receipts.legacy_verifier,
        conf.subgraph_service,
    )));

    // Phase 7: Initialize auth service
    let auth_service = init_auth_service(
        http_client.clone(),
        &kafka_consumer,
        conf.api_keys,
        conf.payment_required,
    )
    .await
    .expect("failed to start auth service");

    // Phase 8: Create budget controller and reporter (static for Axum state lifetime)
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

    // Phase 9: Build query handler context
    let ctx = Context {
        indexer_client,
        receipt_signer,
        budgeter,
        chains: Box::leak(Box::new(Chains::new(conf.chain_aliases))),
        grt_per_usd,
        indexing_perf,
        network,
        attestation_domain,
        legacy_attestation_domain,
        reporter,
    };

    // Phase 10: Start servers
    let blocklist: watch::Receiver<Vec<BlocklistEntry>> = indexer_blocklist.blocklist;
    start_metrics_server(conf.port_metrics);
    let router = build_router(ctx, auth_service, signer_address, blocklist);

    // Switch to graceful shutdown via Axum
    setup_termination.abort();
    start_api_server(router, conf.port_api).await;
    tracing::warn!("shutdown");
}

// =============================================================================
// Initialization Helpers
// =============================================================================

/// Load configuration from CLI argument path.
fn load_configuration() -> config::Config {
    let conf_path = env::args()
        .nth(1)
        .expect("Missing argument for config path")
        .parse::<PathBuf>()
        .unwrap();
    config::load_from_file(&conf_path).expect("Failed to load config")
}

/// Create HTTP client with 20-second timeout.
fn init_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap()
}

/// Initialize exchange rate provider (RPC or fixed value).
async fn init_exchange_rate(
    provider: &ExchangeRateProvider,
) -> watch::Receiver<ordered_float::NotNan<f64>> {
    match provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => watch::channel(*grt_per_usd).1,
        ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url.clone()).await,
    }
}

/// Create EIP-712 attestation domains.
///
/// Returns static references because Axum state requires `'static` lifetime.
/// These domains are immutable config and never deallocated.
fn init_attestation_domains(
    config: &config::AttestationConfig,
) -> (&'static Eip712Domain, &'static Eip712Domain) {
    let chain_id = config
        .chain_id
        .parse::<ChainId>()
        .expect("failed to parse attestation domain chain_id");

    let attestation_domain: &'static Eip712Domain = Box::leak(Box::new(
        attestation::eip712_domain(chain_id, config.dispute_manager),
    ));

    let legacy_attestation_domain: &'static Eip712Domain = Box::leak(Box::new(
        attestation::eip712_domain(chain_id, config.legacy_dispute_manager),
    ));

    (attestation_domain, legacy_attestation_domain)
}

/// Initialize network topology service and related components.
///
/// Returns:
/// - `NetworkService`: Handle for subgraph/deployment resolution
/// - `Blocklist`: Indexer blocklist with Kafka updates
/// - `IndexingPerformance`: Performance tracking for indexer selection
fn init_network_service(
    http_client: &reqwest::Client,
    indexer_client: &IndexerClient,
    conf: &config::Config,
    kafka_consumer: KafkaConsumer,
) -> (
    network::NetworkService,
    indexer_blocklist::Blocklist,
    IndexingPerformance,
) {
    let network_subgraph_client = SubgraphClient {
        client: indexer_client.clone(),
        indexers: conf.trusted_indexers.clone(),
        latest_block: None,
        page_size: 500,
        max_lag_seconds: conf.network_subgraph_max_lag_seconds,
    };

    let indexer_host_blocklist = match &conf.ip_blocker_db {
        Some(path) => {
            config::load_ip_blocklist_from_file(path).expect("failed to load IP blocker DB")
        }
        None => Default::default(),
    };

    let blocklist = indexer_blocklist::Blocklist::spawn(conf.blocklist.clone(), kafka_consumer);

    let network = network::service::spawn(
        http_client.clone(),
        network_subgraph_client,
        blocklist.clone(),
        conf.min_indexer_version.clone(),
        conf.min_graph_node_version.clone(),
        indexer_host_blocklist,
    );

    let indexing_perf = IndexingPerformance::new(network.clone());

    (network, blocklist, indexing_perf)
}

// =============================================================================
// Router Building
// =============================================================================

/// Build the main API router with all middleware layers.
fn build_router(
    ctx: Context,
    auth_service: AuthContext,
    signer_address: thegraph_core::alloy::primitives::Address,
    blocklist: watch::Receiver<Vec<BlocklistEntry>>,
) -> Router {
    let api = Router::new()
        .route(
            "/deployments/id/{deployment_id}",
            routing::post(client_query::handle_query),
        )
        .route(
            "/deployments/id/{deployment_id}/indexers/id/{indexer}",
            routing::post(client_query::handle_indexer_query),
        )
        .route(
            "/subgraphs/id/{subgraph_id}",
            routing::post(client_query::handle_query),
        )
        .route(
            "/{api_key}/deployments/id/{deployment_id}",
            routing::post(client_query::handle_query),
        )
        .route(
            "/{api_key}/subgraphs/id/{subgraph_id}",
            routing::post(client_query::handle_query),
        )
        .with_state(ctx)
        .layer(
            // ServiceBuilder composes layers top-to-bottom, responses bubble up in reverse
            tower::ServiceBuilder::new()
                .layer(
                    CorsLayer::new()
                        .allow_origin(cors::Any)
                        .allow_headers(cors::Any)
                        .allow_methods([http::Method::OPTIONS, http::Method::POST]),
                )
                .layer(RequestTracingLayer::new(format!("{signer_address:?}")))
                .layer(axum::middleware::from_fn(legacy_auth_adapter))
                .layer(RequireAuthorizationLayer::new(auth_service)),
        );

    Router::new()
        .route("/", routing::get(|| async { "Ready to roll!" }))
        .route("/ready", routing::get(|| async { "Ready" }))
        .route(
            "/blocklist",
            routing::get(move || async move { axum::Json(blocklist.borrow().clone()) }),
        )
        .nest("/api", api)
}

// =============================================================================
// Server Startup
// =============================================================================

/// Start the metrics server on a separate port.
///
/// Runs in a background task. The metrics endpoint is not exposed publicly.
fn start_metrics_server(port: u16) {
    tokio::spawn(async move {
        let router = Router::new().route("/metrics", routing::get(handle_metrics));
        let listener =
            TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port))
                .await
                .expect("Failed to bind metrics server");
        axum::serve(listener, router.into_make_service())
            .await
            .expect("Failed to start metrics server");
    });
}

/// Start the main API server with graceful shutdown support.
async fn start_api_server(router: Router, port: u16) {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port))
        .await
        .expect("Failed to bind API server")
        // Disable Nagle's algorithm for lower latency
        .tap_io(|stream| {
            let _ = stream.set_nodelay(true);
        });

    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(await_shutdown_signals())
    .await
    .expect("Failed to start API server");
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
        return (StatusCode::INTERNAL_SERVER_ERROR, Default::default());
    }
    (StatusCode::OK, String::from_utf8(buffer).unwrap())
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
    kafka: &KafkaConsumer,
    config: ApiKeys,
    payment_required: bool,
) -> anyhow::Result<AuthContext> {
    let special_api_keys = match &config {
        ApiKeys::Endpoint { special, .. } => Arc::new(HashSet::from_iter(special.clone())),
        ApiKeys::KakfaTopic { special, .. } => Arc::new(HashSet::from_iter(special.clone())),
        ApiKeys::Fixed(_) => Default::default(),
    };

    let api_keys = match config {
        ApiKeys::Endpoint { url, auth, .. } => auth::studio_api::api_keys(http, url, auth).await,
        ApiKeys::KakfaTopic {
            topic,
            bootstrap_url,
            bootstrap_auth,
            ..
        } => auth::kafka::api_keys(kafka, &topic, &bootstrap_url, &bootstrap_auth).await?,
        ApiKeys::Fixed(api_keys) => {
            let api_keys = api_keys.into_iter().map(|k| (k.key.clone(), k)).collect();
            watch::channel(api_keys).1
        }
    };

    Ok(AuthContext {
        payment_required,
        api_keys,
        special_api_keys,
    })
}

#[derive(Clone)]
struct KafkaConsumer {
    config: KafkaConfig,
    group_id: String,
}

impl KafkaConsumer {
    pub fn new(config: KafkaConfig) -> Self {
        let group_id = format!("gateway-{:x}", rand::rng().next_u64());
        Self { config, group_id }
    }

    pub fn stream_consumer(
        &self,
        topic: &str,
    ) -> anyhow::Result<rdkafka::consumer::StreamConsumer> {
        let consumer: rdkafka::consumer::StreamConsumer =
            rdkafka::config::ClientConfig::from(self.config.clone())
                .remove("compression.codec")
                .remove("queue.buffering.max.messages")
                .remove("queue.buffering.max.ms")
                .remove("message.timeout.ms")
                .set("group.id", &self.group_id)
                .set("auto.offset.reset", "beginning")
                .create()?;
        kafka::assign_partitions(&consumer, topic, rdkafka::Offset::Beginning)?;
        Ok(consumer)
    }
}
