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
use eventuals::{Eventual, EventualExt as _, Ptr};
use gateway_common::types::Indexing;
use gateway_framework::{
    budgets::{Budgeter, USD},
    chains::Chains,
    ip_blocker::IpBlocker,
    ipfs, json,
    network::{
        discovery::Status, exchange_rate, indexing_performance::IndexingPerformance,
        network_subgraph,
    },
    reporting::{self, EventFilterFn, EventHandlerFn, KafkaClient, LoggingOptions},
    scalar::{self, ReceiptSigner},
    subscriptions::subgraph as subscriptions_subgraph,
    topology::network::{Deployment, GraphNetwork},
};
use graph_gateway::{
    client_query::{
        self, auth::AuthContext, context::Context, legacy_auth_adapter::legacy_auth_adapter,
        query_id::SetQueryIdLayer, query_tracing::QueryTracingLayer,
        rate_limiter::AddRateLimiterLayer, require_auth::RequireAuthorizationLayer,
    },
    config::{ApiKeys, Config, ExchangeRateProvider},
    indexer_client::IndexerClient,
    indexers::indexing,
    indexings_blocklist::{self, indexings_blocklist},
    reports::{
        report_client_query, report_indexer_query, CLIENT_QUERY_TARGET, INDEXER_QUERY_TARGET,
    },
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
use tokio::{net::TcpListener, signal::unix::SignalKind, spawn};
use tower_http::cors::{self, CorsLayer};
use uuid::Uuid;

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

    let config_repr = format!("{config:#?}");

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
            event_filter: EventFilterFn::new(|metadata| {
                (metadata.target() == CLIENT_QUERY_TARGET)
                    || (metadata.target() == INDEXER_QUERY_TARGET)
            }),
            event_handler: EventHandlerFn::new(|client, metadata, fields| {
                match metadata.target() {
                    CLIENT_QUERY_TARGET => report_client_query(client, fields),
                    INDEXER_QUERY_TARGET => report_indexer_query(client, fields),
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

    let grt_per_usd: Eventual<NotNan<f64>> = match config.exchange_rate_provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => {
            Eventual::from_value(NotNan::new(grt_per_usd).expect("NAN exchange rate"))
        }
        ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).unwrap(),
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

    let ipfs = ipfs::Client::new(http_client.clone(), config.ipfs, 50);
    let ip_blocker = IpBlocker::new(config.ip_blocker_db.as_deref()).unwrap();
    let network = GraphNetwork::new(subgraphs, ipfs, ip_blocker).await;

    // Indexer blocklist
    // Periodically check the defective POIs list against the network indexers and update the
    // indexers blocklist accordingly.
    let indexings_blocklist = if !config.poi_blocklist.is_empty() {
        let pois = config.poi_blocklist.clone();
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
        Box::leak(Box::new(config.bad_indexers.into_iter().collect()));

    let indexing_statuses = indexing::statuses(
        network.deployments.clone(),
        http_client.clone(),
        config.min_graph_node_version,
        config.min_indexer_version,
    )
    .await;

    let legacy_indexers = indexing_statuses.clone().map(|statuses| async move {
        let legacy_indexers: HashSet<Address> = statuses
            .iter()
            .filter(|(_, status)| status.legacy_scalar)
            .map(|(indexing, _)| indexing.indexer)
            .collect();
        Ptr::new(legacy_indexers)
    });
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
            legacy_indexers,
        )
        .await,
    ));

    eventuals::join((network.deployments.clone(), indexing_statuses.clone()))
        .pipe_async(move |(deployments, indexing_statuses)| async move {
            update_allocations(receipt_signer, &deployments, &indexing_statuses).await;
        })
        .forever();

    let special_api_keys = match &config.api_keys {
        Some(ApiKeys::Endpoint { special, .. }) => HashSet::from_iter(special.clone()),
        _ => Default::default(),
    };
    let api_keys = match config.api_keys {
        Some(ApiKeys::Endpoint { url, auth, .. }) => {
            subgraph_studio::api_keys(http_client.clone(), url, auth.0)
        }
        Some(ApiKeys::Fixed(api_keys)) => Eventual::from_value(Ptr::new(
            api_keys
                .into_iter()
                .map(|k| (k.key.clone(), k.into()))
                .collect(),
        )),
        None => Eventual::from_value(Ptr::default()),
    };

    let subscriptions = match &config.subscriptions {
        None => Eventual::from_value(Ptr::default()),
        Some(subscriptions) => subscriptions_subgraph::Client::create(
            subgraph_client::Client::builder(http_client.clone(), subscriptions.subgraph.clone())
                .with_auth_token(subscriptions.ticket.clone())
                .build(),
        ),
    };
    let auth_handler = AuthContext::create(
        config.payment_required,
        api_keys.clone(),
        special_api_keys,
        subscriptions,
        config
            .subscriptions
            .iter()
            .flat_map(|s| s.special_signers.clone())
            .collect(),
        config
            .subscriptions
            .as_ref()
            .map(|s| s.rate_per_query)
            .unwrap_or(0),
        config
            .subscriptions
            .iter()
            .flat_map(|s| &s.domains)
            .map(|d| (d.chain_id, d.contract))
            .collect(),
    );
    let query_fees_target =
        USD(NotNan::new(config.query_fees_target).expect("invalid query_fees_target"));
    let budgeter: &'static Budgeter = Box::leak(Box::new(Budgeter::new(query_fees_target)));

    grt_per_usd.value().await.unwrap();
    api_keys.value().await.unwrap();

    let client_query_ctx = Context {
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        receipt_signer,
        kafka_client,
        budgeter,
        indexer_selection_retry_limit: config.indexer_selection_retry_limit,
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
            // TODO: Wait until https://github.com/tokio-rs/axum/pull/2653 is released
            // // disable Nagel's algorithm
            // .tcp_nodelay(true)
            .await
            .expect("Failed to start metrics server");
    });

    let rate_limiter_slots = 10;
    let rate_limiter: &'static RateLimiter<String> =
        Box::leak(Box::new(RateLimiter::<String>::new(
            rate_limiter_slots * config.ip_rate_limit as usize,
            rate_limiter_slots,
        )));
    eventuals::timer(Duration::from_secs(1))
        .pipe(|_| rate_limiter.rotate_slots())
        .forever();

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
                .layer(QueryTracingLayer::new(config.graph_env_id.clone()))
                // Set the query ID on the request
                .layer(SetQueryIdLayer::new(gateway_id))
                // Handle legacy in-path auth, and convert it into a header
                .layer(middleware::from_fn(legacy_auth_adapter))
                // Require the query to be authorized
                .layer(RequireAuthorizationLayer::new(auth_handler))
                // Check the query rate limit with a 60s reset interval
                .layer(AddRateLimiterLayer::default()),
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
    // TODO: Wait until https://github.com/tokio-rs/axum/pull/2653 is released
    // // disable Nagel's algorithm
    // .tcp_nodelay(true)
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
