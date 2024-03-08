use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::read_to_string;
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_sol_types::Eip712Domain;
use anyhow::{self, Context as _};
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, status::StatusCode, Request},
    middleware,
    response::Response,
    routing, Router, Server,
};
use eventuals::{Eventual, EventualExt as _, Ptr};
use gateway_framework::geoip::GeoIp;
use ordered_float::NotNan;
use prometheus::{self, Encoder as _};
use secp256k1::SecretKey;
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use thegraph_core::{
    client as subgraph_client,
    types::{attestation, DeploymentId},
};
use tokio::signal::unix::SignalKind;
use tokio::spawn;
use tower_http::cors::{self, CorsLayer};
use uuid::Uuid;

use gateway_common::types::Indexing;
use gateway_framework::budgets::USD;
use gateway_framework::chains::blockmeta;
use gateway_framework::scalar::ReceiptSigner;
use gateway_framework::{
    budgets::Budgeter,
    chains::{ethereum, BlockCache},
    ipfs, json,
    network::{exchange_rate, network_subgraph},
    scalar,
};
use graph_gateway::client_query::auth::AuthContext;
use graph_gateway::client_query::context::Context;
use graph_gateway::client_query::legacy_auth_adapter::legacy_auth_adapter;
use graph_gateway::client_query::query_id::SetQueryIdLayer;
use graph_gateway::client_query::query_tracing::QueryTracingLayer;
use graph_gateway::client_query::rate_limiter::AddRateLimiterLayer;
use graph_gateway::client_query::require_auth::RequireAuthorizationLayer;
use graph_gateway::config::chains::RpcConfig;
use graph_gateway::config::{ApiKeys, Config, ExchangeRateProvider};
use graph_gateway::indexer_client::IndexerClient;
use graph_gateway::indexers::indexing;
use graph_gateway::indexing_performance::IndexingPerformance;
use graph_gateway::indexings_blocklist::indexings_blocklist;
use graph_gateway::reports::{self, KafkaClient};
use graph_gateway::topology::{Deployment, GraphNetwork};
use graph_gateway::{client_query, indexings_blocklist, subgraph_studio, subscriptions_subgraph};

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

    reports::init(kafka_client, config.log_json);
    tracing::info!("Graph gateway starting... ID: {}", gateway_id);
    tracing::debug!(config = %config_repr);

    let geoip = config
        .geoip_database
        .filter(|_| !config.geoip_blocked_countries.is_empty())
        .map(|db| {
            GeoIp::new(db, config.geoip_blocked_countries)
                .context("GeoIp")
                .unwrap()
        });

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let block_caches: HashMap<String, &'static BlockCache> = config
        .chains
        .into_iter()
        .flat_map(|chain| {
            // Build a block cache for each chain
            let cache = new_block_cache(chain.names.clone(), chain.rpc.clone());
            let cache: &'static BlockCache = Box::leak(Box::new(cache));

            chain.names.into_iter().map(move |alias| (alias, cache))
        })
        .collect();
    let block_caches: &'static HashMap<String, &'static BlockCache> =
        Box::leak(Box::new(block_caches));

    let grt_per_usd: Eventual<NotNan<f64>> = match config.exchange_rate_provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => {
            Eventual::from_value(NotNan::new(grt_per_usd).expect("NAN exchange rate"))
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

    let ipfs = ipfs::Client::new(http_client.clone(), config.ipfs, 50);
    let network = GraphNetwork::new(subgraphs, ipfs, geoip).await;

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
            subgraph_studio::api_keys(http_client.clone(), url, auth)
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
        api_keys,
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

    tracing::info!("Waiting for exchange rate...");
    grt_per_usd.value().await.unwrap();

    let client_query_ctx = Context {
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        receipt_signer,
        kafka_client,
        budgeter,
        indexer_selection_retry_limit: config.indexer_selection_retry_limit,
        l2_gateway: config.l2_gateway,
        grt_per_usd,
        network,
        indexing_perf: IndexingPerformance::new(indexing_statuses.clone()),
        indexing_statuses,
        attestation_domain,
        bad_indexers,
        indexings_blocklist,
        block_caches,
    };

    for (chain, block_cache) in block_caches {
        if block_cache.chain_head.value_immediate().is_none() {
            tracing::error!(%chain, "missing chain head");
        }
    }

    // Host metrics on a separate server with a port that isn't open to public requests.
    let metrics_port = config.port_metrics;
    spawn(async move {
        let router = Router::new().route("/metrics", routing::get(handle_metrics));

        Server::bind(&SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            metrics_port,
        ))
        // disable Nagel's algorithm
        .tcp_nodelay(true)
        .serve(router.into_make_service())
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
            routing::post(scalar::handle_collect_receipts).with_state(legacy_signer),
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

    Server::bind(&SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        config.port_api,
    ))
    // disable Nagel's algorithm
    .tcp_nodelay(true)
    .serve(router.into_make_service_with_connect_info::<SocketAddr>())
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

async fn ip_rate_limit<B>(
    State(limiter): State<&'static RateLimiter<String>>,
    ConnectInfo(info): ConnectInfo<SocketAddr>,
    req: Request<B>,
    next: middleware::Next<B>,
) -> Result<Response, json::JsonResponse> {
    if limiter.check_limited(info.ip().to_string()) {
        return Err(graphql_error_response("Too many requests, try again later"));
    }
    Ok(next.run(req).await)
}

async fn update_allocations(
    receipt_signer: &ReceiptSigner,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    indexing_statuses: &HashMap<Indexing, indexing::Status>,
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

/// Returns a new block cache client based on the given configuration.
fn new_block_cache(names: Vec<String>, config: RpcConfig) -> BlockCache {
    match config {
        RpcConfig::Ethereum { rpc_url } => BlockCache::new::<ethereum::Client>(ethereum::Config {
            names,
            url: rpc_url,
        }),
        RpcConfig::Blockmeta { rpc_url, rpc_auth } => {
            BlockCache::new::<blockmeta::Client>(blockmeta::Config {
                names,
                uri: rpc_url.as_str().parse().expect("invalid URI"),
                auth: rpc_auth,
            })
        }
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

fn graphql_error_response<S: ToString>(message: S) -> json::JsonResponse {
    json::json_response([], json!({"errors": [{"message": message.to_string()}]}))
}
