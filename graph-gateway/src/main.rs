mod auth;
mod block_constraints;
mod chains;
mod client_query;
mod config;
mod exchange_rate;
mod fisherman_client;
mod geoip;
mod indexer_client;
mod indexing;
mod ipfs;
mod metrics;
mod network_subgraph;
mod price_automation;
mod receipts;
mod reports;
mod subgraph_client;
mod subgraph_studio;
mod subscriptions;
mod subscriptions_subgraph;
mod topology;
mod unattestable_errors;
mod vouchers;

use anyhow::{self, anyhow};
use auth::AuthHandler;
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, header, status::StatusCode, HeaderMap, HeaderName, HeaderValue, Request},
    middleware,
    response::Response,
    routing, Json, Router, Server,
};
use chains::*;
use config::*;
use eventuals::EventualExt as _;
use fisherman_client::*;
use geoip::GeoIP;
use graph_subscriptions::{subscription_tier::SubscriptionTiers, TicketVerificationDomain};
use indexer_client::IndexerClient;
use indexer_selection::{
    actor::{IndexerUpdate, Update},
    BlockStatus, IndexerInfo, Indexing,
};
use indexing::indexing_statuses;
use indexing::IndexingStatus;
use prelude::{
    anyhow::Context,
    buffer_queue::{self, QueueWriter},
    *,
};
use price_automation::QueryBudgetFactors;
use prometheus::{self, Encoder as _};
use receipts::ReceiptPools;
use reports::KafkaClient;
use secp256k1::SecretKey;
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    env,
    fs::read_to_string,
    io::Write as _,
    iter,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::spawn;
use topology::{Allocation, Deployment, GraphNetwork};
use tower_http::cors::{self, CorsLayer};

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

    let kafka_client = match KafkaClient::new(&config.kafka.into()) {
        Ok(kafka_client) => Box::leak(Box::new(kafka_client)),
        Err(kafka_client_err) => {
            tracing::error!(%kafka_client_err);
            return;
        }
    };

    reports::init(kafka_client, config.log_json);
    tracing::info!("Graph gateway starting...");
    tracing::debug!(config = %config_repr);

    let (isa_state, mut isa_writer) = double_buffer!(indexer_selection::State::default());

    if let Some(path) = &config.restricted_deployments {
        let restricted_deployments =
            load_restricted_deployments(path).expect("Failed to load restricted deployments");
        tracing::debug!(?restricted_deployments);
        isa_writer
            .update(|indexers| indexers.restricted_deployments = restricted_deployments.clone())
            .await;
    }

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
        .timeout(Duration::from_secs(30))
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
    let l2_migration_delay = config
        .l2_migration_delay_hours
        .map(|hours| chrono::Duration::hours(hours as i64));
    let network_subgraph_data =
        network_subgraph::Client::create(network_subgraph_client, l2_migration_delay)
            .await
            .unwrap();

    update_writer
        .write(Update::SlashingPercentage(
            network_subgraph_data.network_params.slashing_percentage,
        ))
        .unwrap();

    let receipt_pools: &'static ReceiptPools = Box::leak(Box::default());

    let ipfs = ipfs::Client::new(http_client.clone(), config.ipfs, 50);
    let network = GraphNetwork::new(network_subgraph_data.subgraphs, ipfs).await;

    let indexing_statuses = indexing_statuses(
        network.deployments.clone(),
        http_client.clone(),
        config.min_indexer_version,
        geoip,
    )
    .await;
    {
        let update_writer = update_writer.clone();
        eventuals::join((network.deployments.clone(), indexing_statuses))
            .pipe_async(move |(deployments, indexing_statuses)| {
                let update_writer = update_writer.clone();
                async move {
                    write_indexer_inputs(
                        &signer_key,
                        block_caches,
                        &update_writer,
                        receipt_pools,
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

    let subscription_tiers: &'static SubscriptionTiers =
        Box::leak(Box::new(config.subscription_tiers));

    let subscriptions = match config.subscriptions_subgraph {
        None => Eventual::from_value(Ptr::default()),
        Some(subgraph_endpoint) => subscriptions_subgraph::Client::create(
            subgraph_client::Client::new(
                http_client.clone(),
                subgraph_endpoint,
                config.subscriptions_ticket,
            ),
            config.subscriptions_owner,
            subscription_tiers,
        ),
    };
    let subscriptions_domain_separator =
        match (config.subscriptions_chain_id, config.subscriptions_contract) {
            (Some(chain_id), Some(contract)) => Some(TicketVerificationDomain {
                contract: contract.0.into(),
                chain_id: chain_id.into(),
            }),
            (_, _) => None,
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
        subscriptions_domain_separator,
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
        fisherman_client,
        block_caches,
        observations: update_writer,
        receipt_pools,
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
            CorsLayer::new()
                .allow_origin(cors::Any)
                .allow_headers(cors::Any)
                .allow_methods([http::Method::POST]),
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

fn load_restricted_deployments(
    path: &Path,
) -> anyhow::Result<Arc<HashMap<DeploymentId, HashSet<Address>>>> {
    read_to_string(path)?
        .split('\n')
        .filter(|l| l.trim_end() != "")
        .map(|line| {
            let mut csv = line.split_terminator(',');
            let deployment = csv.next()?.parse().ok()?;
            let indexers = csv.map(|i| i.parse().ok()).collect::<Option<_>>()?;
            Some((deployment, indexers))
        })
        .collect::<Option<_>>()
        .map(Arc::new)
        .ok_or(anyhow!("malformed payload"))
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
    json_response([], response)
}

async fn write_indexer_inputs(
    signer: &SecretKey,
    block_caches: &HashMap<String, BlockCache>,
    update_writer: &QueueWriter<Update>,
    receipt_pools: &ReceiptPools,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    indexing_statuses: &HashMap<Indexing, IndexingStatus>,
) {
    tracing::info!(
        deployments = deployments.len(),
        allocations = deployments
            .values()
            .map(|d| d.allocations.len())
            .sum::<usize>(),
        indexing_statuses = indexing_statuses.len(),
    );

    let mut indexers: HashMap<Address, IndexerUpdate> = deployments
        .values()
        .flat_map(|deployment| &deployment.allocations)
        .map(|Allocation { indexer, .. }| {
            let update = IndexerUpdate {
                info: Arc::new(IndexerInfo {
                    stake: indexer.staked_tokens,
                    url: indexer.url.clone(),
                }),
                indexings: HashMap::new(),
            };
            (indexer.id, update)
        })
        .collect();

    let mut latest_blocks = HashMap::<String, u64>::new();
    for (indexing, status) in indexing_statuses {
        let indexer = match indexers.get_mut(&indexing.indexer) {
            Some(indexer) => indexer,
            None => continue,
        };
        let latest = match latest_blocks.entry(status.chain.clone()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => *entry.insert(
                block_caches
                    .get(&status.chain)
                    .and_then(|cache| cache.chain_head.value_immediate().map(|b| b.number))
                    .unwrap_or(0),
            ),
        };
        let allocations: HashMap<Address, GRT> = deployments
            .get(&indexing.deployment)
            .into_iter()
            .flat_map(|deployment| &deployment.allocations)
            .filter(|Allocation { indexer, .. }| indexer.id == indexing.indexer)
            .map(
                |Allocation {
                     id,
                     allocated_tokens,
                     ..
                 }| (*id, *allocated_tokens),
            )
            .collect();

        receipt_pools
            .update_receipt_pool(signer, indexing, &allocations)
            .await;

        indexer.indexings.insert(
            indexing.deployment,
            indexer_selection::IndexingStatus {
                allocations: Arc::new(allocations),
                cost_model: status.cost_model.clone(),
                block: Some(BlockStatus {
                    reported_number: status.block.number,
                    blocks_behind: latest.saturating_sub(status.block.number),
                    behind_reported_block: false,
                    min_block: status.min_block,
                }),
            },
        );
    }

    let _ = update_writer.write(Update::Indexers(indexers));
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

pub type JsonResponse = (HeaderMap, Json<serde_json::Value>);

pub fn json_response<H>(headers: H, payload: serde_json::Value) -> JsonResponse
where
    H: IntoIterator<Item = (HeaderName, HeaderValue)>,
{
    let headers = HeaderMap::from_iter(
        iter::once((
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        ))
        .chain(headers),
    );
    (headers, Json(payload))
}

pub fn graphql_error_response<S: ToString>(message: S) -> JsonResponse {
    json_response([], json!({"errors": [{"message": message.to_string()}]}))
}
