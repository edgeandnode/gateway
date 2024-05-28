use std::{
    collections::{HashMap, HashSet},
    env,
    fs::read_to_string,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{Address, U256};
use alloy_sol_types::Eip712Domain;
use anyhow::{self, Context as _};
use axum::{
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, status::StatusCode, Request},
    middleware,
    middleware::Next,
    response::Response,
    routing, Router,
};
use config::{ApiKeys, Config, ExchangeRateProvider};
use eventuals::{Eventual, EventualExt as _, Ptr};
use gateway_common::types::Indexing;
use gateway_framework::{
    auth::AuthContext,
    budgets::{Budgeter, USD},
    chains::Chains,
    http::middleware::{
        legacy_auth_adapter, RequestTracingLayer, RequireAuthorizationLayer, SetRequestIdLayer,
    },
    ip_blocker::IpBlocker,
    json,
    network::{
        discovery::Status, exchange_rate, indexing_performance::IndexingPerformance,
        network_subgraph,
    },
    reporting::{
        self, EventHandlerFn, KafkaClient, LoggingOptions, CLIENT_REQUEST_TARGET,
        INDEXER_REQUEST_TARGET,
    },
    scalar::{self, ReceiptSigner},
    topology::network::{Deployment, GraphNetwork},
};
use graph_gateway::{
    client_query::{self, context::Context},
    indexer_client::IndexerClient,
    indexers,
    indexers::indexing,
    indexings_blocklist::{self, indexings_blocklist},
    reports::{report_client_query, report_indexer_query},
    subgraph_studio,
};
use ordered_float::NotNan;
use prometheus::{self, Encoder as _};
use secp256k1::SecretKey;
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use thegraph_core::{
    client as subgraph_client,
    types::{attestation, DeploymentId},
};
use tokio::{
    net::TcpListener,
    signal::unix::SignalKind,
    spawn,
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
    let config_path = env::args()
        .nth(1)
        .expect("Missing argument for config path")
        .parse::<PathBuf>()
        .unwrap();
    let config_file_text = read_to_string(config_path.clone()).expect("Failed to open config");
    let config = serde_json::from_str::<Config>(&config_file_text)
        .context("Failed to parse JSON config")
        .unwrap();

    // Get the gateway ID from the config or generate a new one.
    let gateway_id = config
        .gateway_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let config_repr = format!("{config:?}");

    // Instantiate the Kafka client
    let kafka_client: &'static KafkaClient = match KafkaClient::new(&config.kafka.into()) {
        Ok(kafka_client) => Box::leak(Box::new(kafka_client)),
        Err(kafka_client_err) => {
            tracing::error!(%kafka_client_err);
            return;
        }
    };

    // Initialize logging
    reporting::init(
        kafka_client,
        LoggingOptions {
            executable_name: "graph-gateway".into(),
            json: config.log_json,
            event_handler: EventHandlerFn::new(|client, metadata, fields| {
                match metadata.target() {
                    CLIENT_REQUEST_TARGET => report_client_query(client, fields),
                    INDEXER_REQUEST_TARGET => report_indexer_query(client, fields),
                    _ => unreachable!("invalid event target for KafkaLayer"),
                }
            }),
        },
    );

    tracing::info!("gateway ID: {}", gateway_id);
    tracing::debug!(config = %config_repr);

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let grt_per_usd: watch::Receiver<NotNan<f64>> = match config.exchange_rate_provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => {
            watch::channel(NotNan::new(grt_per_usd).expect("NAN exchange rate")).1
        }
        ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await.unwrap(),
    };

    let network_subgraph_client =
        subgraph_client::Client::new(http_client.clone(), config.network_subgraph.clone());
    let subgraphs =
        network_subgraph::Client::create(network_subgraph_client, config.l2_gateway.is_some())
            .await;

    let attestation_domain: &'static Eip712Domain =
        Box::leak(Box::new(attestation::eip712_domain(
            U256::from_str_radix(&config.attestations.chain_id, 10)
                .expect("failed to parse attestation domain chain_id"),
            config.attestations.dispute_manager,
        )));

    let ip_blocker = IpBlocker::new(config.ip_blocker_db.as_deref()).unwrap();
    let network = GraphNetwork::new(subgraphs, ip_blocker).await;

    // Indexer blocklist
    // Periodically check the defective POIs list against the network indexers and update the
    // indexers blocklist accordingly.
    let indexings_blocklist = if !config.poi_blocklist.is_empty() {
        let pois = config.poi_blocklist.into_iter().map(Into::into).collect();
        let update_interval = config
            .poi_blocklist_update_interval
            .map_or(indexings_blocklist::DEFAULT_UPDATE_INTERVAL, |min| {
                Duration::from_secs(min * 60)
            });

        indexings_blocklist(
            http_client.clone(),
            network.deployments.clone(),
            network.indexers.clone(),
            pois,
            update_interval,
        )
        .await
    } else {
        Eventual::from_value(Ptr::default())
    };

    let bad_indexers: &'static HashSet<Address> =
        Box::leak(Box::new(FromIterator::from_iter(config.bad_indexers)));

    let indexing_statuses = indexing::statuses(
        network.deployments.clone(),
        http_client.clone(),
        config.min_graph_node_version,
        config.min_indexer_version,
    )
    .await;

    let legacy_signer: &'static SecretKey = Box::leak(Box::new(
        config
            .scalar
            .legacy_signer
            .map(|s| s.0)
            .unwrap_or(config.scalar.signer.0),
    ));
    let receipt_signer: &'static ReceiptSigner = Box::leak(Box::new(
        ReceiptSigner::new(
            config.scalar.signer.0,
            config.scalar.chain_id,
            config.scalar.verifier,
            legacy_signer,
        )
        .await,
    ));

    eventuals::join((network.deployments.clone(), indexing_statuses.clone()))
        .pipe_async(move |(deployments, indexing_statuses)| async move {
            update_allocations(receipt_signer, &deployments, &indexing_statuses).await;
        })
        .forever();

    let auth_service = init_auth_service(
        http_client.clone(),
        config.api_keys,
        config.payment_required,
    )
    .await;

    let query_fees_target =
        USD(NotNan::new(config.query_fees_target).expect("invalid query_fees_target"));
    let budgeter: &'static Budgeter = Box::leak(Box::new(Budgeter::new(query_fees_target)));

    let client_query_ctx = Context {
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        receipt_signer,
        kafka_client,
        budgeter,
        l2_gateway: config.l2_gateway,
        chains: Box::leak(Box::new(Chains::new(config.chain_aliases))),
        grt_per_usd,
        network,
        indexing_perf: IndexingPerformance::new(indexing_statuses.clone()),
        indexing_statuses,
        attestation_domain,
        bad_indexers,
        indexings_blocklist,
    };

    // Host metrics on a separate server with a port that isn't open to public requests.
    let metrics_port = config.port_metrics;
    spawn(async move {
        let router = Router::new().route("/metrics", routing::get(handle_metrics));

        let metrics_listener = TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            metrics_port,
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
            rate_limiter_slots * config.ip_rate_limit as usize,
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
        .with_state(client_query_ctx)
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
                .layer(RequestTracingLayer::new(config.graph_env_id.clone()))
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
            routing::post(scalar::handle_collect_receipts)
                .with_state(legacy_signer)
                .layer(DefaultBodyLimit::max(3_000_000)),
        )
        .route(
            "/partial-voucher",
            routing::post(scalar::handle_partial_voucher)
                .with_state(legacy_signer)
                .layer(DefaultBodyLimit::max(3_000_000)),
        )
        .route(
            "/voucher",
            routing::post(scalar::handle_voucher).with_state(legacy_signer),
        )
        .route(
            "/budget",
            routing::get(|| async { budgeter.query_fees_target.0.to_string() }),
        )
        .nest("/api", api)
        .layer(middleware::from_fn_with_state(rate_limiter, ip_rate_limit));

    let app_listener = TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        config.port_api,
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

async fn update_allocations(
    receipt_signer: &ReceiptSigner,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    indexing_statuses: &HashMap<Indexing, Status>,
) {
    tracing::info!(
        deployments = deployments.len(),
        indexings = deployments
            .values()
            .map(|d| d.indexers.len())
            .sum::<usize>(),
        indexing_statuses = indexing_statuses.len(),
    );

    let mut allocations: HashMap<Indexing, Address> = HashMap::new();
    for (deployment, indexer) in deployments.values().flat_map(|deployment| {
        deployment
            .indexers
            .values()
            .map(|indexer| (deployment.as_ref(), indexer.as_ref()))
    }) {
        let indexing = Indexing {
            indexer: indexer.id,
            deployment: deployment.id,
        };
        allocations.insert(indexing, indexer.largest_allocation);
    }
    receipt_signer.update_allocations(allocations).await;
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
/// This functions awaits the completion of the initial API keys and subscriptions fetches.
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

// Mapping between config and internal types
impl From<config::ProofOfIndexingInfo> for indexers::public_poi::ProofOfIndexingInfo {
    fn from(value: config::ProofOfIndexingInfo) -> Self {
        indexers::public_poi::ProofOfIndexingInfo {
            proof_of_indexing: value.proof_of_indexing,
            deployment_id: value.deployment_id,
            block_number: value.block_number,
        }
    }
}
