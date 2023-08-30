use std::time::Duration;
use std::{
    collections::hash_map::{Entry, HashMap},
    collections::hash_set::HashSet,
    env,
    fs::read_to_string,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};

use alloy_primitives::Address;
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
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use tokio::spawn;
use toolshed::thegraph::DeploymentId;
use tower_http::cors::{self, CorsLayer};

use graph_gateway::indexings_blocklist::indexings_blocklist;
use graph_gateway::{
    auth::AuthHandler,
    chains::{ethereum, BlockCache},
    client_query,
    config::{Config, ExchangeRateProvider},
    fisherman_client::FishermanClient,
    geoip::GeoIP,
    indexer_client::IndexerClient,
    indexing::{indexing_statuses, IndexingStatus},
    indexings_blocklist, ipfs, network_subgraph,
    price_automation::QueryBudgetFactors,
    receipts::ReceiptSigner,
    reports,
    reports::KafkaClient,
    subgraph_client, subgraph_studio, subscriptions_subgraph,
    topology::{Deployment, GraphNetwork},
    vouchers, JsonResponse,
};
use indexer_selection::{actor::Update, BlockStatus, Indexing};
use prelude::buffer_queue::QueueWriter;
use prelude::{buffer_queue, double_buffer};

// Moving the `exchange_rate` module to `lib.rs` makes the doctests to fail during the compilation
// step. This module is only used here, so let's keep it here for now.
mod exchange_rate;

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
    let kafka_client = match KafkaClient::new(&config.kafka.into()) {
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

    let block_caches = config
        .chains
        .into_iter()
        .map(|chain| {
            let network = chain.name.clone();
            let cache = BlockCache::new::<ethereum::Client>(chain.block_rate_hz, chain.into());
            (network, cache)
        })
        .collect::<HashMap<String, BlockCache>>();
    let block_caches: &'static HashMap<String, BlockCache> = Box::leak(Box::new(block_caches));
    let signer_key = config.signer_key.0;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let usd_to_grt = match config.exchange_rate_provider {
        ExchangeRateProvider::Fixed(usd_to_grt) => Eventual::from_value(usd_to_grt),
        ExchangeRateProvider::Rpc(url) => exchange_rate::usd_to_grt(url).await.unwrap(),
    };
    update_from_eventual(
        usd_to_grt.clone(),
        update_writer.clone(),
        Update::USDToGRTConversion,
    );

    let network_subgraph_client =
        subgraph_client::Client::new(http_client.clone(), config.network_subgraph.clone(), None);
    let network_subgraph_data =
        network_subgraph::Client::create(network_subgraph_client, config.l2_gateway.is_some())
            .await
            .unwrap();

    update_writer
        .write(Update::SlashingPercentage(
            network_subgraph_data.network_params.slashing_percentage,
        ))
        .unwrap();

    let receipt_signer: &'static ReceiptSigner =
        Box::leak(Box::new(ReceiptSigner::new(signer_key)));

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

    let indexing_statuses = indexing_statuses(
        network.deployments.clone(),
        http_client.clone(),
        config.min_indexer_version,
        geoip,
    )
    .await;

    {
        let update_writer = update_writer.clone();
        let indexing_statuses = indexing_statuses.clone();
        eventuals::join((network.deployments.clone(), indexing_statuses))
            .pipe_async(move |(deployments, indexing_statuses)| {
                let update_writer = update_writer.clone();
                async move {
                    write_indexer_inputs(
                        block_caches,
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

    let subscriptions = match config.subscriptions {
        None => Eventual::from_value(Ptr::default()),
        Some(subscriptions) => subscriptions_subgraph::Client::create(
            subgraph_client::Client::new(
                http_client.clone(),
                subscriptions.subgraph,
                subscriptions.ticket,
            ),
            subscription_tiers,
            subscriptions.contract_owners,
        ),
    };
    let auth_handler = AuthHandler::create(
        QueryBudgetFactors {
            scale: config.query_budget_scale,
            discount: config.query_budget_discount,
            processes: config.gateway_instance_count as f64,
        },
        api_keys,
        HashSet::from_iter(config.special_api_keys),
        config.api_key_payment_required,
        subscriptions,
    );

    let fisherman_client = config.fisherman.map(|url| {
        Box::leak(Box::new(FishermanClient::new(http_client.clone(), url)))
            as &'static FishermanClient
    });

    tracing::info!("Waiting for exchange rate...");
    usd_to_grt.value().await.unwrap();
    tracing::info!("Waiting for ISA setup...");
    update_writer.flush().await.unwrap();

    let client_query_ctx = client_query::Context {
        indexer_selection_retry_limit: config.indexer_selection_retry_limit,
        l2_gateway: config.l2_gateway,
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        graph_env_id: config.graph_env_id.clone(),
        auth_handler,
        network,
        indexing_statuses,
        indexings_blocklist,
        fisherman_client,
        block_caches,
        observations: update_writer,
        receipt_signer,
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
            routing::post(vouchers::handle_collect_receipts).with_state(signer_key),
        )
        .route(
            "/partial-voucher",
            routing::post(vouchers::handle_partial_voucher)
                .with_state(signer_key)
                .layer(DefaultBodyLimit::max(3_000_000)),
        )
        .route(
            "/voucher",
            routing::post(vouchers::handle_voucher).with_state(signer_key),
        )
        // Temporary route. Will be replaced by gateway metadata (GSP).
        .route(
            "/subscription-tiers",
            routing::get(handle_subscription_tiers).with_state(subscription_tiers),
        )
        .nest("/api", api)
        .layer(middleware::from_fn_with_state(rate_limiter, ip_rate_limit));

    Server::bind(&SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        api_port,
    ))
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
) -> Result<Response, JsonResponse> {
    if limiter.check_limited(info.ip().to_string()) {
        return Err(graphql_error_response("Too many requests, try again later"));
    }
    Ok(next.run(req).await)
}

async fn handle_subscription_tiers(
    State(tiers): State<&'static SubscriptionTiers>,
) -> JsonResponse {
    let response = serde_json::to_value(tiers.as_ref()).unwrap();
    graph_gateway::json_response([], response)
}

async fn write_indexer_inputs(
    block_caches: &HashMap<String, BlockCache>,
    update_writer: &QueueWriter<Update>,
    receipt_signer: &ReceiptSigner,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    indexing_statuses: &HashMap<Indexing, IndexingStatus>,
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
    let mut latest_blocks: HashMap<String, u64> = HashMap::new();
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
        let latest_block = match latest_blocks.entry(status.chain.clone()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => *entry.insert(
                block_caches
                    .get(&status.chain)
                    .and_then(|cache| cache.chain_head.value_immediate().map(|b| b.number))
                    .unwrap_or(0),
            ),
        };
        let update = indexer_selection::IndexingStatus {
            url: indexer.url.clone(),
            stake: indexer.staked_tokens,
            allocation: indexer.allocated_tokens,
            cost_model: status.cost_model.clone(),
            block: Some(BlockStatus {
                reported_number: status.block.number,
                blocks_behind: latest_block.saturating_sub(status.block.number),
                behind_reported_block: false,
                min_block: status.min_block,
            }),
            version: deployment.version.clone(),
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

fn graphql_error_response<S: ToString>(message: S) -> JsonResponse {
    graph_gateway::json_response([], json!({"errors": [{"message": message.to_string()}]}))
}
