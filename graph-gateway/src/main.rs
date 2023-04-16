mod auth;
mod block_constraints;
mod chains;
mod client_query;
mod config;
mod fisherman_client;
mod geoip;
mod indexer_client;
mod indexer_status;
mod ipfs_client;
mod manifest_client;
mod metrics;
mod network_subgraph;
mod price_automation;
mod receipts;
mod reports;
mod studio_client;
mod subgraph_client;
mod subgraph_deployments;
mod subscriptions;
mod subscriptions_subgraph;
mod unattestable_errors;
mod vouchers;

use crate::{
    auth::AuthHandler, chains::*, config::*, fisherman_client::*, geoip::GeoIP,
    indexer_client::IndexerClient, indexer_status::IndexingStatus, ipfs_client::*,
    price_automation::QueryBudgetFactors, receipts::ReceiptPools, reports::KafkaClient,
};
use anyhow::{self, anyhow};
use axum::{
    extract::{ConnectInfo, State},
    http::{self, header, status::StatusCode, HeaderMap, HeaderName, HeaderValue, Request},
    middleware,
    response::Response,
    routing, Json, Router, Server,
};
use eventuals::EventualExt as _;
use graph_subscriptions::TicketVerificationDomain;
use indexer_selection::{
    actor::{IndexerUpdate, Update},
    BlockStatus, IndexerInfo, Indexing,
};
use network_subgraph::AllocationInfo;
use prelude::{
    anyhow::Context,
    buffer_queue::{self, QueueWriter},
    *,
};
use prometheus::{self, Encoder as _};
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

    let studio_data =
        studio_client::Actor::create(http_client.clone(), config.studio_url, config.studio_auth);
    update_from_eventual(
        studio_data.usd_to_grt,
        update_writer.clone(),
        Update::USDToGRTConversion,
    );

    let network_subgraph_client =
        subgraph_client::Client::new(http_client.clone(), config.network_subgraph.clone());
    let l2_migration_delay = config
        .l2_migration_delay_hours
        .map(|hours| chrono::Duration::hours(hours as i64));
    let network_subgraph_data =
        network_subgraph::Client::create(network_subgraph_client, l2_migration_delay);
    update_from_eventual(
        network_subgraph_data.slashing_percentage,
        update_writer.clone(),
        Update::SlashingPercentage,
    );

    let receipt_pools: &'static ReceiptPools = Box::leak(Box::default());

    let indexer_status_data = indexer_status::Actor::create(
        config.min_indexer_version,
        geoip,
        network_subgraph_data.indexers.clone(),
    );
    {
        let update_writer = update_writer.clone();
        eventuals::join((
            network_subgraph_data.allocations.clone(),
            network_subgraph_data.indexers,
            indexer_status_data.indexings,
        ))
        .pipe_async(move |(allocations, indexer_info, indexing_statuses)| {
            let update_writer = update_writer.clone();
            async move {
                write_indexer_inputs(
                    &signer_key,
                    block_caches,
                    &update_writer,
                    receipt_pools,
                    &allocations,
                    &indexer_info,
                    &indexing_statuses,
                )
                .await;
            }
        })
        .forever();
    }

    let deployment_ids = network_subgraph_data
        .deployment_indexers
        .clone()
        .map(|deployments| async move { deployments.keys().cloned().collect() });
    let ipfs_client = IPFSClient::new(http_client.clone(), config.ipfs, 50);
    let subgraph_info = manifest_client::create(
        ipfs_client,
        network_subgraph_data.subgraph_deployments.clone(),
        deployment_ids,
    );

    let subscriptions = match config.subscriptions_subgraph {
        None => Eventual::from_value(Ptr::default()),
        Some(subgraph_endpoint) => subscriptions_subgraph::Client::create(
            subgraph_client::Client::new(http_client.clone(), subgraph_endpoint),
            config.subscription_tiers,
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
        studio_data.api_keys,
        HashSet::from_iter(config.special_api_keys),
        config.api_key_payment_required,
        subscriptions,
        subscriptions_domain_separator,
    );

    let fisherman_client = config.fisherman.map(|url| {
        Box::leak(Box::new(FishermanClient::new(http_client.clone(), url)))
            as &'static FishermanClient
    });

    let client_query_ctx = client_query::Context {
        indexer_selection_retry_limit: config.indexer_selection_retry_limit,
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        graph_env_id: config.graph_env_id.clone(),
        auth_handler,
        subgraph_info,
        subgraph_deployments: network_subgraph_data.subgraph_deployments,
        deployment_indexers: network_subgraph_data.deployment_indexers,
        fisherman_client,
        block_caches,
        observations: update_writer,
        receipt_pools,
        isa_state,
    };
    let ready_data = ReadyData {
        start_time: Instant::now(),
        block_caches,
        allocations: network_subgraph_data.allocations,
    };

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
        // This subgraphs route is a footgun on the network. We should eventually deprecate it.
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
        .route("/ready", routing::get(handle_ready).with_state(ready_data))
        .route(
            "/collect-receipts",
            routing::post(vouchers::handle_collect_receipts).with_state(signer_key),
        )
        .route(
            "/partial-voucher",
            routing::post(vouchers::handle_partial_voucher).with_state(signer_key),
        )
        .route(
            "/voucher",
            routing::post(vouchers::handle_voucher).with_state(signer_key),
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

async fn write_indexer_inputs(
    signer: &SecretKey,
    block_caches: &HashMap<String, BlockCache>,
    update_writer: &QueueWriter<Update>,
    receipt_pools: &ReceiptPools,
    allocations: &HashMap<Address, AllocationInfo>,
    indexer_info: &HashMap<Address, Arc<IndexerInfo>>,
    indexing_statuses: &HashMap<Indexing, IndexingStatus>,
) {
    tracing::info!(
        allocations = allocations.len(),
        indexers = indexer_info.len(),
        indexing_statuses = indexing_statuses.len(),
    );

    let mut indexers = indexer_info
        .iter()
        .map(|(indexer, info)| {
            let update = IndexerUpdate {
                info: info.clone(),
                indexings: HashMap::new(),
            };
            (*indexer, update)
        })
        .collect::<HashMap<Address, IndexerUpdate>>();

    let mut latest_blocks = HashMap::<String, u64>::new();
    for (indexing, status) in indexing_statuses {
        let indexer = match indexers.get_mut(&indexing.indexer) {
            Some(indexer) => indexer,
            None => continue,
        };
        let latest = match latest_blocks.entry(status.network.clone()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => *entry.insert(
                block_caches
                    .get(&status.network)
                    .and_then(|cache| cache.chain_head.value_immediate().map(|b| b.number))
                    .unwrap_or(0),
            ),
        };
        let allocations = allocations
            .iter()
            .filter(|(_, info)| &info.indexing == indexing)
            .map(|(id, info)| (*id, info.allocated_tokens))
            .collect::<HashMap<Address, GRT>>();

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

#[derive(Clone)]
struct ReadyData {
    start_time: Instant,
    block_caches: &'static HashMap<String, BlockCache>,
    allocations: Eventual<Ptr<HashMap<Address, AllocationInfo>>>,
}

async fn handle_ready(State(data): State<ReadyData>) -> impl axum::response::IntoResponse {
    // Wait for 30 seconds since startup for subgraph manifests to load.
    let timer_ready = data.start_time.elapsed() > Duration::from_secs(30);
    let block_caches_ready = data
        .block_caches
        .iter()
        .all(|(_, cache)| cache.chain_head.value_immediate().is_some());
    let allocations_ready = data
        .allocations
        .value_immediate()
        .map(|map| map.len())
        .unwrap_or(0)
        > 0;
    if timer_ready && block_caches_ready && allocations_ready {
        (StatusCode::OK, "Ready")
    } else {
        // Respond with 425 Too Early
        (StatusCode::from_u16(425).unwrap(), "Not ready")
    }
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
    json_response([], json!({"errors": {"message": message.to_string()}}))
}
