mod block_constraints;
mod chains;
mod fisherman_client;
mod geoip;
mod indexer_client;
mod indexer_status;
mod ipfs_client;
mod kafka_client;
mod manifest_client;
mod metrics;
mod network_subgraph;
mod opt;
mod price_automation;
mod query_engine;
mod rate_limiter;
mod receipts;
mod studio_client;
mod subgraph_deployments;
#[cfg(test)]
mod tests;
mod unattestable_errors;
mod vouchers;
use crate::{
    chains::*,
    fisherman_client::*,
    geoip::GeoIP,
    indexer_client::IndexerClient,
    indexer_status::IndexingStatus,
    ipfs_client::*,
    kafka_client::{ClientQueryResult, IndexerAttempt, KafkaClient, KafkaInterface as _},
    manifest_client::*,
    metrics::*,
    opt::*,
    price_automation::QueryBudgetFactors,
    query_engine::*,
    rate_limiter::*,
    receipts::ReceiptPools,
    subgraph_deployments::SubgraphDeployments,
};
use actix_cors::Cors;
use actix_web::{
    dev::ServiceRequest,
    http::{header, StatusCode},
    web, App, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer,
};
use eventuals::EventualExt as _;
use indexer_selection::{
    actor::{IndexerUpdate, Update},
    BlockStatus, IndexerInfo, Indexing,
};
use network_subgraph::AllocationInfo;
use prelude::{
    buffer_queue::{self, QueueWriter},
    double_buffer::DoubleBufferReader,
    *,
};
use prometheus::{self, Encoder as _};
use reqwest;
use secp256k1::SecretKey;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use simple_rate_limiter::RateLimiter;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
    time::SystemTime,
};
use structopt::StructOpt as _;
use tokio::spawn;
use url::Url;

#[actix_web::main]
async fn main() {
    let opt = Opt::from_args();
    init_tracing(opt.log_json);
    tracing::info!("Graph gateway starting...");
    tracing::debug!("{:#?}", opt);

    let kafka_client = match KafkaClient::new(&opt.kafka_config()) {
        Ok(kafka_client) => Arc::new(kafka_client),
        Err(kafka_client_err) => {
            tracing::error!(%kafka_client_err);
            return;
        }
    };

    let (isa_state, mut isa_writer) = double_buffer!(indexer_selection::State::default());
    let special_indexers = if opt.mips.0.len() > 0 {
        Some(Arc::new(opt.mips.0))
    } else {
        None
    };
    let _ = isa_writer
        .update(|indexers| indexers.special_indexers = special_indexers.clone())
        .await;

    // Start the actor to manage updates
    let (update_writer, update_reader) = buffer_queue::pair();
    spawn(async move {
        indexer_selection::actor::process_updates(isa_writer, update_reader).await;
        tracing::error!("ISA actor stopped");
    });

    let geoip = opt
        .geoip_database
        .filter(|_| !opt.geoip_blocked_countries.is_empty())
        .map(|db| GeoIP::new(db, opt.geoip_blocked_countries).unwrap());

    let block_caches = opt
        .ethereum_providers
        .0
        .into_iter()
        .map(|provider| {
            let network = provider.network.clone();
            let cache = BlockCache::new::<ethereum::Client>(provider);
            (network, cache)
        })
        .collect::<HashMap<String, BlockCache>>();
    let block_caches = Arc::new(block_caches);
    let signer_key = opt.signer_key.0;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let studio_data =
        studio_client::Actor::create(http_client.clone(), opt.studio_url, opt.studio_auth);
    update_from_eventual(
        studio_data.usd_to_grt,
        update_writer.clone(),
        Update::USDToGRTConversion,
    );

    let network_subgraph_data =
        network_subgraph::Client::create(http_client.clone(), opt.network_subgraph.clone());
    update_from_eventual(
        network_subgraph_data.slashing_percentage,
        update_writer.clone(),
        Update::SlashingPercentage,
    );

    let receipt_pools = ReceiptPools::default();

    let indexer_status_data = indexer_status::Actor::create(
        opt.min_indexer_version,
        geoip,
        network_subgraph_data.indexers.clone(),
    );
    {
        let signer_key = signer_key.clone();
        let receipt_pools = receipt_pools.clone();
        let block_caches = block_caches.clone();
        let update_writer = update_writer.clone();
        eventuals::join((
            network_subgraph_data.allocations.clone(),
            network_subgraph_data.indexers,
            indexer_status_data.indexings,
        ))
        .pipe_async(move |(allocations, indexer_info, indexing_statuses)| {
            let signer_key = signer_key.clone();
            let receipt_pools = receipt_pools.clone();
            let block_caches = block_caches.clone();
            let update_writer = update_writer.clone();
            async move {
                write_indexer_inputs(
                    &signer_key,
                    &block_caches,
                    &update_writer,
                    &receipt_pools,
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
    let ipfs_client = IPFSClient::new(http_client.clone(), opt.ipfs, 5);
    let subgraph_info = manifest_client::create(
        ipfs_client,
        network_subgraph_data.subgraph_deployments.clone(),
        deployment_ids,
    );

    let special_api_keys = Arc::new(HashSet::from_iter(opt.special_api_keys));

    let fisherman_client = opt
        .fisherman
        .map(|url| Arc::new(FishermanClient::new(http_client.clone(), url)));
    let subgraph_query_data = SubgraphQueryData {
        config: query_engine::Config {
            indexer_selection_retry_limit: opt.indexer_selection_retry_limit,
            budget_factors: QueryBudgetFactors {
                scale: opt.query_budget_scale,
                discount: opt.query_budget_discount,
                processes: (opt.replica_count * opt.location_count) as f64,
            },
        },
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        subgraph_info,
        subgraph_deployments: network_subgraph_data.subgraph_deployments,
        deployment_indexers: network_subgraph_data.deployment_indexers,
        api_keys: studio_data.api_keys,
        api_key_payment_required: opt.api_key_payment_required,
        fisherman_client,
        kafka_client,
        block_caches: block_caches.clone(),
        observations: update_writer,
        receipt_pools,
        isa_state,
        special_api_keys,
    };
    let network_subgraph_query_data = NetworkSubgraphQueryData {
        http_client,
        network_subgraph: opt.network_subgraph,
        network_subgraph_auth_token: opt.network_subgraph_auth_token,
    };
    let ready_data = ReadyData {
        block_caches,
        allocations: network_subgraph_data.allocations,
    };

    let metrics_port = opt.metrics_port;
    // Host metrics on a separate server with a port that isn't open to public requests.
    actix_web::rt::spawn(async move {
        HttpServer::new(move || App::new().route("/metrics", web::get().to(handle_metrics)))
            .workers(1)
            .bind(("0.0.0.0", metrics_port))
            .expect("Failed to bind to metrics port")
            .run()
            .await
            .expect("Failed to start metrics server")
    });
    let ip_rate_limiter = RateLimiter::<String>::new(
        opt.ip_rate_limit as usize,
        opt.ip_rate_limit_window_secs as usize,
    );
    let api_rate_limiter = RateLimiter::<String>::new(
        opt.api_rate_limit as usize,
        opt.api_rate_limit_window_secs as usize,
    );
    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_header()
            .allowed_methods(vec!["POST", "OPTIONS"]);
        let api = web::scope("/api/{api_key}")
            .wrap(cors)
            .wrap(RateLimiterMiddleware {
                rate_limiter: api_rate_limiter.clone(),
                key: request_api_key,
            })
            .app_data(web::Data::new(subgraph_query_data.clone()))
            .app_data(web::JsonConfig::default().error_handler(|err, _| {
                actix_web::error::InternalError::from_response(
                    err,
                    graphql_error_response("Invalid query"),
                )
                .into()
            }))
            .route(
                "/subgraphs/id/{subgraph_id}",
                web::post().to(handle_subgraph_query),
            )
            .route(
                "/deployments/id/{deployment_id}",
                web::post().to(handle_subgraph_query),
            );
        let other = web::scope("")
            .wrap(RateLimiterMiddleware {
                rate_limiter: ip_rate_limiter.clone(),
                key: request_host,
            })
            .route("/", web::get().to(|| async { "Ready to roll!" }))
            .service(
                web::resource("/ready")
                    .app_data(web::Data::new(ready_data.clone()))
                    .route(web::get().to(handle_ready)),
            )
            .service(
                web::resource("/network")
                    .app_data(web::Data::new(network_subgraph_query_data.clone()))
                    .route(web::post().to(handle_network_query)),
            )
            .service(
                web::resource("/collect-receipts")
                    // TODO: decrease payload limit
                    .app_data(web::PayloadConfig::new(16_000_000))
                    .app_data(web::Data::new(signer_key.clone()))
                    .route(web::post().to(vouchers::handle_collect_receipts)),
            )
            .service(
                web::resource("/partial-voucher")
                    .app_data(web::PayloadConfig::new(4_000_000))
                    .app_data(web::Data::new(signer_key.clone()))
                    .route(web::post().to(vouchers::handle_partial_voucher)),
            )
            .service(
                web::resource("/voucher")
                    .app_data(web::Data::new(signer_key.clone()))
                    .route(web::post().to(vouchers::handle_voucher)),
            );
        App::new().service(api).service(other)
    })
    .bind(("0.0.0.0", opt.port))
    .expect("Failed to bind")
    .run()
    .await
    .expect("Failed to start server");
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

fn request_api_key(request: &ServiceRequest) -> String {
    format!(
        "{}/{}",
        request_host(request),
        request.match_info().get("api_key").unwrap_or("")
    )
}

fn request_host(request: &ServiceRequest) -> String {
    let info = request.connection_info();
    info.realip_remote_addr()
        .map(|addr|
        // Trim port number
        &addr[0..addr.rfind(":").unwrap_or(addr.len())])
        // Fallback to hostname
        .unwrap_or_else(|| info.host())
        .to_string()
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
            (indexer.clone(), update)
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
            .map(|(id, info)| (id.clone(), info.allocated_tokens.clone()))
            .collect::<HashMap<Address, GRT>>();

        receipt_pools
            .update_receipt_pool(signer, indexing, &allocations)
            .await;

        indexer.indexings.insert(
            indexing.deployment.clone(),
            indexer_selection::IndexingStatus {
                allocations: Arc::new(allocations),
                cost_model: status.cost_model.clone(),
                block: Some(BlockStatus {
                    reported_number: status.block.number,
                    blocks_behind: latest.saturating_sub(status.block.number),
                    behind_reported_block: false,
                }),
            },
        );
    }

    let _ = update_writer.write(Update::Indexers(indexers));
}

#[tracing::instrument]
async fn handle_metrics() -> HttpResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    if let Err(metrics_encode_err) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(%metrics_encode_err);
        return HttpResponseBuilder::new(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to encode metrics");
    }
    HttpResponseBuilder::new(StatusCode::OK).body(buffer)
}

#[derive(Clone)]
struct ReadyData {
    block_caches: Arc<HashMap<String, BlockCache>>,
    allocations: Eventual<Ptr<HashMap<Address, AllocationInfo>>>,
}

async fn handle_ready(data: web::Data<ReadyData>) -> HttpResponse {
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
    if block_caches_ready && allocations_ready {
        HttpResponseBuilder::new(StatusCode::OK).body("Ready")
    } else {
        // Respond with 425 Too Early
        HttpResponseBuilder::new(StatusCode::from_u16(425).unwrap()).body("Not ready")
    }
}

#[derive(Clone)]
struct NetworkSubgraphQueryData {
    http_client: reqwest::Client,
    network_subgraph: URL,
    network_subgraph_auth_token: String,
}

#[tracing::instrument(skip(payload, data))]
async fn handle_network_query(
    _: HttpRequest,
    payload: String,
    data: web::Data<NetworkSubgraphQueryData>,
) -> HttpResponse {
    let _timer = METRICS.network_subgraph.duration.start_timer();
    let post_request = |body: String| async {
        let response = data
            .http_client
            .post(data.network_subgraph.0.clone())
            .body(body)
            .header(header::CONTENT_TYPE.as_str(), "application/json")
            .header(
                "Authorization",
                format!("Bearer {}", data.network_subgraph_auth_token),
            )
            .send()
            .await?;
        tracing::info!(network_subgraph_response = %response.status());
        response.text().await
    };
    match post_request(payload).await {
        Ok(result) => {
            METRICS.network_subgraph.ok.inc();
            HttpResponseBuilder::new(StatusCode::OK).body(result)
        }
        Err(network_subgraph_post_err) => {
            tracing::error!(%network_subgraph_post_err);
            METRICS.network_subgraph.err.inc();
            graphql_error_response("Failed to process network subgraph query")
        }
    }
}

#[derive(Deserialize, Debug)]
struct QueryBody {
    query: String,
    variables: Option<Box<RawValue>>,
}

#[derive(Clone)]
struct SubgraphQueryData {
    config: Config,
    indexer_client: IndexerClient,
    fisherman_client: Option<Arc<FishermanClient>>,
    block_caches: Arc<HashMap<String, BlockCache>>,
    subgraph_info: SubgraphInfoMap,
    subgraph_deployments: SubgraphDeployments,
    deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    receipt_pools: ReceiptPools,
    isa_state: DoubleBufferReader<indexer_selection::State>,
    observations: QueueWriter<Update>,
    api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    api_key_payment_required: bool,
    kafka_client: Arc<KafkaClient>,
    special_api_keys: Arc<HashSet<String>>,
}

#[derive(Debug)]
enum SubgraphResolutionError {
    InvalidSubgraphID(String),
    InvalidDeploymentID(String),
    SubgraphNotFound(String),
    DeploymentNotFound(String),
}

impl SubgraphQueryData {
    async fn resolve_subgraph_deployment(
        &self,
        params: &actix_web::dev::Path<actix_web::dev::Url>,
    ) -> Result<Ptr<SubgraphInfo>, SubgraphResolutionError> {
        let deployment = if let Some(id) = params.get("subgraph_id") {
            let subgraph = id
                .parse::<SubgraphID>()
                .map_err(|_| SubgraphResolutionError::InvalidSubgraphID(id.to_string()))?;
            self.subgraph_deployments
                .current_deployment(&subgraph)
                .await
                .ok_or_else(|| SubgraphResolutionError::SubgraphNotFound(id.to_string()))?
        } else if let Some(id) = params.get("deployment_id") {
            SubgraphDeploymentID::from_ipfs_hash(id)
                .ok_or_else(|| SubgraphResolutionError::InvalidDeploymentID(id.to_string()))?
        } else {
            return Err(SubgraphResolutionError::SubgraphNotFound("".to_string()));
        };
        self.subgraph_info
            .value_immediate()
            .and_then(|map| map.get(&deployment)?.value_immediate())
            .map(move |info| info)
            .ok_or_else(|| SubgraphResolutionError::DeploymentNotFound(deployment.to_string()))
    }
}

async fn handle_subgraph_query(
    request: HttpRequest,
    payload: web::Json<QueryBody>,
    data: web::Data<SubgraphQueryData>,
) -> HttpResponse {
    let ray_id = request
        .headers()
        .get("cf-ray")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let variables = payload.variables.as_ref().map(ToString::to_string);
    let mut query = Query::new(ray_id, payload.into_inner().query, variables);
    // We check that the requested subgraph is valid now, since we don't want to log query info for
    // unknown subgraphs requests.
    query.subgraph = match data.resolve_subgraph_deployment(request.match_info()).await {
        Ok(result) => Some(result),
        Err(subgraph_resolution_err) => {
            tracing::info!(?subgraph_resolution_err);
            return graphql_error_response(format!("{:?}", subgraph_resolution_err));
        }
    };
    let span = tracing::info_span!(
        "handle_subgraph_query",
        ray_id = %query.ray_id,
        query_id = %query.id,
        subgraph = %query.subgraph.as_ref().unwrap().id,
        deployment = %query.subgraph.as_ref().unwrap().deployment,
        network = %query.subgraph.as_ref().unwrap().network,
    );
    let api_key = request.match_info().get("api_key").unwrap_or("");

    let response = handle_subgraph_query_inner(&request, &data, &mut query, api_key)
        .instrument(span)
        .await;

    let (payload, status_result) = match response {
        Ok(payload) => (payload, Ok(StatusCode::OK.to_string())),
        Err(msg) => (graphql_error_response(&msg), Err(msg)),
    };
    notify_query_result(&data.kafka_client, &query, status_result);

    payload
}

async fn handle_subgraph_query_inner(
    request: &HttpRequest,
    data: &web::Data<SubgraphQueryData>,
    query: &mut Query,
    api_key: &str,
) -> Result<HttpResponse, String> {
    let query_engine = QueryEngine {
        config: data.config.clone(),
        indexer_client: data.indexer_client.clone(),
        kafka_client: data.kafka_client.clone(),
        fisherman_client: data.fisherman_client.clone(),
        deployment_indexers: data.deployment_indexers.clone(),
        block_caches: data.block_caches.clone(),
        receipt_pools: data.receipt_pools.clone(),
        isa: data.isa_state.clone(),
        observations: data.observations.clone(),
    };
    let api_keys = data.api_keys.value_immediate().unwrap_or_default();
    query.api_key = api_keys.get(api_key).cloned();
    let api_key = match &query.api_key {
        Some(api_key) => api_key.clone(),
        None => return Err("Invalid API key".into()),
    };

    if data.api_key_payment_required
        && !api_key.is_subsidized
        && !data.special_api_keys.contains(&api_key.key)
    {
        // Enforce the API key payment status, unless it's being subsidized.
        match api_key.query_status {
            QueryStatus::Active => (),
            QueryStatus::Inactive => return Err(
                "Querying not activated yet; make sure to add some GRT to your balance in the studio"
                    .into(),
            ),
            QueryStatus::ServiceShutoff => {
                return Err("Payment required for subsequent requests for this API key".into())
            }
        };
    }

    let domain = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Some(v.parse::<Url>().ok()?.host_str()?.to_string()))
        .unwrap_or("".to_string());
    tracing::debug!(%domain, authorized = ?api_key.domains);
    let authorized_domains = api_key.domains.iter().map(|(d, _)| d.as_str());
    if !api_key.domains.is_empty() && !is_domain_authorized(authorized_domains, &domain) {
        return Err("Domain not authorized by API key".into());
    }

    let deployment = &query.subgraph.as_ref().unwrap().deployment.clone();
    let deployment_authorized =
        api_key.deployments.is_empty() || api_key.deployments.contains(&deployment);
    let subgraph = &query.subgraph.as_ref().unwrap().id;
    let subgraph_authorized =
        api_key.subgraphs.is_empty() || api_key.subgraphs.iter().any(|(s, _)| s == subgraph);
    if !deployment_authorized || !subgraph_authorized {
        return Err("Subgraph not authorized by API key".into());
    }

    if let Err(err) = query_engine.execute_query(query).await {
        return Err(match err {
            QueryEngineError::MalformedQuery => "Invalid query".into(),
            QueryEngineError::NoIndexers => "No indexers found for subgraph deployment".into(),
            QueryEngineError::NoIndexerSelected => {
                format!(
                    "No suitable indexer found for subgraph deployment, {} indexers attempted",
                    query.indexer_attempts.len()
                )
            }
            QueryEngineError::FeesTooHigh(count) => {
                format!(
                    "No suitable indexer found, {} indexers requesting higher fees for this query",
                    count
                )
            }
            QueryEngineError::BlockBeforeMin => {
                "Requested block before minimum `startBlock` of subgraph manifest".into()
            }
            QueryEngineError::NetworkNotSupported(network) => {
                format!("Network not supported: {}", network)
            }
            QueryEngineError::MissingBlock(_) => "Gateway failed to resolve required blocks".into(),
            QueryEngineError::MissingNetworkParams => "Internal error: MissingNetworkParams".into(),
            QueryEngineError::MissingExchangeRate => "Internal error: MissingExchangeRate".into(),
            QueryEngineError::ExcessiveFee => "Internal error: ExcessiveFee".into(),
        });
    }
    let last_attempt = query.indexer_attempts.last().unwrap();
    let response = last_attempt.result.as_ref().unwrap();

    let attestation = response
        .attestation
        .as_ref()
        .and_then(|attestation| serde_json::to_string(attestation).ok())
        .unwrap_or_default();
    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .insert_header(header::ContentType::json())
        .insert_header(("Graph-Attestation", attestation))
        .body(&response.payload))
}

pub fn graphql_error_response<S: ToString>(message: S) -> HttpResponse {
    HttpResponseBuilder::new(StatusCode::OK)
        .insert_header(header::ContentType::json())
        .body(json!({"errors": {"message": message.to_string()}}).to_string())
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn is_domain_authorized<'a>(authorized: impl IntoIterator<Item = &'a str>, origin: &str) -> bool {
    authorized.into_iter().any(|authorized| {
        let pattern = authorized.split('.');
        let origin = origin.split('.');
        let count = pattern.clone().count();
        if (count < 1) || (origin.clone().count() != count) {
            return false;
        }
        pattern.zip(origin).all(|(p, o)| (p == o) || (p == "*"))
    })
}

fn notify_query_result(kafka_client: &KafkaClient, query: &Query, result: Result<String, String>) {
    let ts = timestamp();
    let query_result = ClientQueryResult::new(&query, result.clone(), ts);
    kafka_client.send(&query_result);

    let indexer_attempts = query
        .indexer_attempts
        .iter()
        .map(|attempt| IndexerAttempt {
            api_key: query_result.api_key.clone(),
            deployment: query_result.deployment.clone(),
            ray_id: query_result.ray_id.clone(),
            indexer: attempt.indexer.to_string(),
            url: attempt.score.url.to_string(),
            allocation: attempt.allocation.to_string(),
            fee: attempt.score.fee.as_f64(),
            utility: *attempt.score.utility,
            blocks_behind: attempt.score.blocks_behind,
            indexer_errors: attempt.indexer_errors.clone(),
            response_time_ms: attempt.duration.as_millis() as u32,
            status: match &attempt.result {
                Ok(response) => response.status.to_string(),
                Err(err) => format!("{:?}", err),
            },
            status_code: attempt.status_code(),
            timestamp: ts,
        })
        .collect::<Vec<IndexerAttempt>>();

    for attempt in indexer_attempts {
        kafka_client.send(&attempt);
    }

    let (status, status_code) = match &result {
        Ok(status) => (status, 0),
        Err(status) => (status, sip24_hash(status) | 0x1),
    };
    let api_key = &query.api_key.as_ref().map(|k| k.key.as_ref()).unwrap_or("");
    let subgraph = query.subgraph.as_ref().unwrap();
    let deployment = subgraph.deployment.to_string();
    // The following logs are required for data science.
    tracing::info!(
        ray_id = %query.ray_id,
        query_id = %query.id,
        %deployment,
        network = %query.subgraph.as_ref().unwrap().network,
        %api_key,
        query = %query.query,
        variables = %query.variables.as_deref().unwrap_or(""),
        budget = %query.budget.as_ref().map(ToString::to_string).unwrap_or_default(),
        fee = query_result.fee,
        response_time_ms = (Instant::now() - query.start_time).as_millis() as u32,
        %status,
        status_code,
        "Client query result",
    );
    for (attempt_index, attempt) in query.indexer_attempts.iter().enumerate() {
        let status = match &attempt.result {
            Ok(response) => response.status.to_string(),
            Err(err) => format!("{:?}", err),
        };
        tracing::info!(
            ray_id = %query.ray_id,
            query_id = %query.id,
            api_key = %api_key,
            %deployment,
            attempt_index,
            indexer = %attempt.indexer,
            url = %attempt.score.url,
            allocation = %attempt.allocation,
            fee = %attempt.score.fee,
            utility = *attempt.score.utility,
            blocks_behind = attempt.score.blocks_behind,
            indexer_errors = %attempt.indexer_errors,
            response_time_ms = attempt.duration.as_millis() as u32,
            %status,
            status_code = attempt.status_code() as u32,
            "Indexer attempt",
        );
    }
}

#[cfg(test)]
mod test {
    use super::is_domain_authorized;

    #[test]
    fn authorized_domains() {
        let authorized_domains = ["example.com", "localhost", "a.b.c", "*.d.e"];
        let tests = [
            ("", false),
            ("example.com", true),
            ("subdomain.example.com", false),
            ("localhost", true),
            ("badhost", false),
            ("a.b.c", true),
            ("c", false),
            ("b.c", false),
            ("d.b.c", false),
            ("a", false),
            ("a.b", false),
            ("e", false),
            ("d.e", false),
            ("z.d.e", true),
        ];
        for (input, expected) in tests {
            assert_eq!(expected, is_domain_authorized(authorized_domains, input));
        }
    }
}
