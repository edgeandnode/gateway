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
use anyhow::{self, Context};
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, status::StatusCode, Request},
    middleware,
    response::Response,
    routing, Router, Server,
};
use eventuals::{Eventual, EventualExt as _, Ptr};
use graph_subscriptions::subscription_tier::SubscriptionTiers;
use prometheus::{self, Encoder as _};
use secp256k1::SecretKey;
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use thegraph::{
    client as subgraph_client,
    types::{attestation, DeploymentId},
};
use tokio::spawn;
use toolshed::{
    buffer_queue::{self, QueueWriter},
    double_buffer,
};
use tower_http::cors::{self, CorsLayer};

use gateway_common::types::{Indexing, GRT, USD};
use gateway_framework::geoip::GeoIP;
use gateway_framework::scalar::ReceiptSigner;
use gateway_framework::{
    chains::{ethereum, BlockCache},
    ipfs, json,
    network::{exchange_rate, network_subgraph},
    scalar,
};
use graph_gateway::auth::AuthHandler;
use graph_gateway::budgets::Budgeter;
use graph_gateway::config::{Config, ExchangeRateProvider};
use graph_gateway::indexer_client::IndexerClient;
use graph_gateway::indexers::indexing;
use graph_gateway::indexings_blocklist::indexings_blocklist;
use graph_gateway::reports::{self, KafkaClient};
use graph_gateway::topology::{Deployment, GraphNetwork};
use graph_gateway::{client_query, indexings_blocklist, subgraph_studio, subscriptions_subgraph};
use indexer_selection::{actor::Update, BlockStatus};

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

    let config_repr = format!("{config:#?}");

    // Instantiate the Kafka client
    let kafka_client: &'static KafkaClient = match KafkaClient::new(&config.kafka.into()) {
        Ok(kafka_client) => Box::leak(Box::new(kafka_client)),
        Err(kafka_client_err) => {
            tracing::error!(%kafka_client_err);
            return;
        }
    };

    reports::init(
        kafka_client,
        config.log_json,
        config
            .subscriptions
            .as_ref()
            .and_then(|s| s.kafka_topic.clone()),
    );
    tracing::info!("Graph gateway starting...");
    tracing::debug!(config = %config_repr);

    let (isa_state, isa_writer) = double_buffer!(indexer_selection::State::default());

    // Start the actor to manage updates
    let (update_writer, update_reader) = buffer_queue::pair();
    spawn(async move {
        indexer_selection::actor::process_updates(isa_writer, update_reader).await;
        tracing::error!("ISA actor stopped");
    });

    let geoip = config
        .geoip_database
        .filter(|_| !config.geoip_blocked_countries.is_empty())
        .map(|db| GeoIP::new(db, config.geoip_blocked_countries).unwrap());

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let block_caches: HashMap<String, &'static BlockCache> = config
        .chains
        .into_iter()
        .flat_map(|chain| {
            let cache: &'static BlockCache =
                Box::leak(Box::new(BlockCache::new::<ethereum::Client>(chain.clone())));
            chain.names.into_iter().map(move |alias| (alias, cache))
        })
        .collect();
    let block_caches: &'static HashMap<String, &'static BlockCache> =
        Box::leak(Box::new(block_caches));

    let grt_per_usd: Eventual<GRT> = match config.exchange_rate_provider {
        ExchangeRateProvider::Fixed(grt_per_usd) => Eventual::from_value(GRT(grt_per_usd)),
        ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await.unwrap(),
    };
    update_from_eventual(
        grt_per_usd.clone(),
        update_writer.clone(),
        Update::GRTPerUSD,
    );

    let network_subgraph_client =
        subgraph_client::Client::new(http_client.clone(), config.network_subgraph.clone());
    let network_subgraph_data =
        network_subgraph::Client::create(network_subgraph_client, config.l2_gateway.is_some())
            .await
            .unwrap();

    update_writer
        .write(Update::SlashingPercentage(
            network_subgraph_data.network_params.slashing_percentage,
        ))
        .unwrap();

    let attestation_domain: &'static Eip712Domain =
        Box::leak(Box::new(attestation::eip712_domain(
            U256::from_str_radix(&config.attestations.chain_id, 10)
                .expect("failed to parse attestation domain chain_id"),
            config.attestations.dispute_manager,
        )));

    let ipfs = ipfs::Client::new(http_client.clone(), config.ipfs, 50);
    let network = GraphNetwork::new(network_subgraph_data.subgraphs, ipfs).await;

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

    let indexing_statuses = indexing::statuses(
        network.deployments.clone(),
        http_client.clone(),
        config.min_indexer_version,
        geoip,
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

    {
        let update_writer = update_writer.clone();
        let indexing_statuses = indexing_statuses.clone();
        eventuals::join((network.deployments.clone(), indexing_statuses))
            .pipe_async(move |(deployments, indexing_statuses)| {
                let update_writer = update_writer.clone();
                async move {
                    write_indexer_inputs(
                        &update_writer,
                        receipt_signer,
                        &deployments,
                        &indexing_statuses,
                    )
                    .await;
                }
            })
            .forever();
    }

    let api_keys = match config.studio_url {
        Some(url) => subgraph_studio::api_keys(http_client.clone(), url, config.studio_auth),
        None => Eventual::from_value(Ptr::default()),
    };

    let subscription_tiers = config.subscriptions.as_ref().map(|s| s.tiers.clone());
    let subscription_tiers: &'static SubscriptionTiers =
        Box::leak(Box::new(subscription_tiers.unwrap_or_default()));

    let subscriptions = match &config.subscriptions {
        None => Eventual::from_value(Ptr::default()),
        Some(subscriptions) => subscriptions_subgraph::Client::create(
            subgraph_client::Client::builder(http_client.clone(), subscriptions.subgraph.clone())
                .with_auth_token(subscriptions.ticket.clone())
                .build(),
        ),
    };
    let auth_handler = AuthHandler::create(
        api_keys,
        HashSet::from_iter(config.special_api_keys),
        config
            .subscriptions
            .iter()
            .flat_map(|s| s.special_signers.clone())
            .collect(),
        config.api_key_payment_required,
        subscriptions,
        subscription_tiers,
        config
            .subscriptions
            .iter()
            .flat_map(|s| &s.domains)
            .map(|d| (d.chain_id, d.contract))
            .collect(),
    );
    let query_fees_target = config
        .query_fees_target
        .try_into()
        .map(USD)
        .expect("invalid query_fees_target");
    let budgeter: &'static Budgeter = Box::leak(Box::new(Budgeter::new(query_fees_target)));

    tracing::info!("Waiting for exchange rate...");
    grt_per_usd.value().await.unwrap();
    tracing::info!("Waiting for ISA setup...");
    update_writer.flush().await.unwrap();

    let client_query_ctx = client_query::Context {
        indexer_selection_retry_limit: config.indexer_selection_retry_limit,
        l2_gateway: config.l2_gateway,
        indexer_client: IndexerClient {
            client: http_client.clone(),
            receipt_signer,
        },
        kafka_client,
        graph_env_id: config.graph_env_id.clone(),
        auth_handler,
        budgeter,
        network,
        indexing_statuses,
        attestation_domain,
        indexings_blocklist,
        block_caches,
        observations: update_writer,
        isa_state,
    };

    tracing::info!("Waiting for chain heads from block caches...");
    for (chain, block_cache) in block_caches {
        let head = block_cache.chain_head.value().await.unwrap();
        tracing::debug!(%chain, ?head);
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

    let api_port = config.port_api;
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
            // bottom, and then the response would bubble back up through the layers in reverse.
            tower::ServiceBuilder::new()
                .layer(
                    CorsLayer::new()
                        .allow_origin(cors::Any)
                        .allow_headers(cors::Any)
                        .allow_methods([http::Method::OPTIONS, http::Method::POST]),
                )
                .layer(middleware::from_fn(client_query::legacy_auth_adapter)),
        );

    let router = Router::new()
        .route("/", routing::get(|| async { "Ready to roll!" }))
        // This path is required by NGINX ingress controller.
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
        // Temporary route. Will be replaced by gateway metadata (GSP).
        .route(
            "/subscription-tiers",
            routing::get(handle_subscription_tiers).with_state(subscription_tiers),
        )
        .route(
            "/budget",
            routing::get(|| async { budgeter.query_fees_target.0.to_string() }),
        )
        .nest("/api", api)
        .layer(middleware::from_fn_with_state(rate_limiter, ip_rate_limit));

    Server::bind(&SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        api_port,
    ))
    // disable Nagel's algorithm
    .tcp_nodelay(true)
    .serve(router.into_make_service_with_connect_info::<SocketAddr>())
    .await
    .expect("Failed to start API server");
}

fn update_from_eventual<V, F>(eventual: Eventual<V>, writer: QueueWriter<Update>, f: F)
where
    V: eventuals::Value,
    F: 'static + Send + Fn(V) -> Update,
{
    eventual
        .pipe(move |v| {
            let _ = writer.write(f(v));
        })
        .forever();
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

async fn handle_subscription_tiers(
    State(tiers): State<&'static SubscriptionTiers>,
) -> json::JsonResponse {
    let response = serde_json::to_value(tiers.as_ref()).unwrap();
    json::json_response([], response)
}

async fn write_indexer_inputs(
    update_writer: &QueueWriter<Update>,
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

    let mut indexings: HashMap<Indexing, indexer_selection::IndexingStatus> = HashMap::new();
    let mut allocations: HashMap<Indexing, Address> = HashMap::new();
    for (deployment, indexer) in deployments.values().flat_map(|deployment| {
        deployment
            .indexers
            .iter()
            .map(move |indexer| (deployment.as_ref(), indexer.as_ref()))
    }) {
        let indexing = Indexing {
            indexer: indexer.id,
            deployment: deployment.id,
        };
        let status = match indexing_statuses.get(&indexing) {
            Some(status) => status,
            None => continue,
        };
        let update = indexer_selection::IndexingStatus {
            url: indexer.url.clone(),
            stake: indexer.staked_tokens,
            allocation: indexer.allocated_tokens,
            block: Some(BlockStatus {
                reported_number: status.block.number,
                behind_reported_block: false,
                min_block: status.min_block,
            }),
        };
        allocations.insert(indexing, indexer.largest_allocation);
        indexings.insert(indexing, update);
    }
    receipt_signer.update_allocations(allocations).await;
    let _ = update_writer.write(Update::Indexings(indexings));
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
