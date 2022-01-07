mod block_resolver;
mod ethereum_client;
mod indexer_client;
mod indexer_selection;
mod ipfs_client;
mod manifest_client;
mod opt;
mod prelude;
mod query_engine;
mod rate_limiter;
mod stats_db;
mod sync_client;
mod ws_client;

use crate::{
    block_resolver::{BlockCache, BlockResolver},
    indexer_client::IndexerClient,
    indexer_selection::{SecretKey, UtilityConfig},
    ipfs_client::*,
    manifest_client::*,
    opt::*,
    prelude::*,
    query_engine::*,
    rate_limiter::*,
};
use actix_cors::Cors;
use actix_web::{
    dev::ServiceRequest,
    http::{header, StatusCode},
    web, App, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer,
};
use eventuals::EventualExt;
use hex;
use lazy_static::lazy_static;
use prometheus::{self, Encoder as _};
use reqwest;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use std::{collections::HashMap, sync::Arc};
use structopt::StructOpt as _;
use url::Url;

#[actix_web::main]
async fn main() {
    let opt = Opt::from_args();
    init_tracing(opt.log_json);
    tracing::info!("Graph gateway starting...");
    tracing::debug!("{:#?}", opt);

    let network = if opt.ethereum_providers.0.len() == 1 {
        opt.ethereum_providers.0[0].network.clone()
    } else {
        tracing::error!("We only support a single Ethereum network provider!");
        return;
    };

    let (input_writers, inputs) = Inputs::new();

    // Trigger decay every 20 minutes.
    let indexer_selection = inputs.indexers.clone();
    eventuals::timer(Duration::from_secs(20 * 60))
        .pipe_async(move |_| {
            let indexer_selection = indexer_selection.clone();
            async move {
                indexer_selection.decay().await;
            }
        })
        .forever();

    let stats_db = match stats_db::create(
        &opt.stats_db_host,
        opt.stats_db_port,
        &opt.stats_db_name,
        &opt.stats_db_user,
        &opt.stats_db_password,
    )
    .await
    {
        Ok(stats_db) => stats_db,
        Err(stats_db_create_err) => {
            tracing::error!(%stats_db_create_err);
            return;
        }
    };
    let block_resolvers = opt
        .ethereum_providers
        .0
        .into_iter()
        .map(|provider| {
            let network = provider.network.clone();
            let (block_cache_writer, block_cache) =
                BlockCache::new(opt.block_cache_head, opt.block_cache_size);
            let chain_client = ethereum_client::create(provider, block_cache_writer);
            let resolver = BlockResolver::new(network.clone(), block_cache, chain_client);
            (network, resolver)
        })
        .collect::<HashMap<String, BlockResolver>>();
    let block_resolvers = Arc::new(block_resolvers);
    let signer_key = opt.signer_key.0;
    let (api_keys_writer, api_keys) = Eventual::new();
    // TODO: argument for timeout
    let sync_metrics = sync_client::create(
        network.clone(),
        opt.sync_agent,
        Duration::from_secs(30),
        signer_key.clone(),
        input_writers,
        block_resolvers.clone(),
        api_keys_writer,
    );
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let ipfs_client = IPFSClient::new(http_client.clone(), opt.ipfs, 5);
    let deployment_ids = inputs
        .deployment_indexers
        .clone()
        .map(|deployments| async move { deployments.keys().cloned().collect() });
    let subgraph_info = manifest_client::create(ipfs_client, deployment_ids);

    let subgraph_query_data = SubgraphQueryData {
        config: query_engine::Config {
            network,
            indexer_selection_retry_limit: opt.indexer_selection_retry_limit,
            utility: UtilityConfig::default(),
            query_budget: opt.query_budget,
        },
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        block_resolvers: block_resolvers.clone(),
        subgraph_info,
        inputs: inputs.clone(),
        api_keys,
        stats_db,
    };
    let network_subgraph_query_data = NetworkSubgraphQueryData {
        http_client,
        network_subgraph: opt.network_subgraph,
        network_subgraph_auth_token: opt.network_subgraph_auth_token,
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
    let ip_rate_limiter = RateLimiter::new(
        Duration::from_secs(opt.ip_rate_limit_window_secs.into()),
        opt.ip_rate_limit as usize,
    );
    let api_rate_limiter = RateLimiter::new(
        Duration::from_secs(opt.api_rate_limit_window_secs.into()),
        opt.api_rate_limit as usize,
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
                    graphql_error_response(StatusCode::OK, "Invalid query"),
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
                    .app_data(web::Data::new((
                        block_resolvers.clone(),
                        sync_metrics.clone(),
                    )))
                    .route(web::get().to(handle_ready)),
            )
            .service(
                web::resource("/network")
                    .app_data(web::Data::new(network_subgraph_query_data.clone()))
                    .route(web::post().to(handle_network_query)),
            )
            .service(
                web::resource("/collect-receipts")
                    .app_data(web::PayloadConfig::new(25_000_000))
                    .app_data(web::Data::new(signer_key.clone()))
                    .route(web::post().to(handle_collect_receipts)),
            );
        App::new().service(api).service(other)
    })
    .bind(("0.0.0.0", opt.port))
    .expect("Failed to bind")
    .run()
    .await
    .expect("Failed to start server");
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

#[tracing::instrument(skip(data))]
async fn handle_ready(
    data: web::Data<(Arc<HashMap<String, BlockResolver>>, sync_client::Metrics)>,
) -> HttpResponse {
    let ready = data
        .0
        .iter()
        .all(|(_, resolver)| resolver.latest_block().is_some())
        && (data.1.allocations.get() > 0);
    if ready {
        HttpResponseBuilder::new(StatusCode::OK).body("Ready")
    } else {
        // Respond with 425 Too Early
        HttpResponseBuilder::new(StatusCode::from_u16(425).unwrap()).body("Not ready")
    }
}

#[tracing::instrument(skip(data, payload))]
async fn handle_collect_receipts(data: web::Data<SecretKey>, payload: web::Bytes) -> HttpResponse {
    let _timer = METRICS.collect_receipts_duration.start_timer();
    if payload.len() < 20 {
        return HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body("Invalid receipt data");
    }
    let mut allocation_id = [0u8; 20];
    allocation_id.copy_from_slice(&payload[..20]);
    let result = indexer_selection::Allocations::receipts_to_voucher(
        &allocation_id.into(),
        data.as_ref(),
        &payload[20..],
    );
    match result {
        Ok(voucher) => {
            METRICS.collect_receipts_ok.inc();
            tracing::info!(request_size = %payload.len(), "Collect receipts");
            HttpResponseBuilder::new(StatusCode::OK).json(json!({
                "allocation": format!("0x{}", hex::encode(voucher.allocation_id)),
                "amount": voucher.fees.to_string(),
                // For now, this must not include a `0x` prefix because the indexer-agent will
                // unconditionally create the prefix when submitting the voucher on chain.
                "signature": hex::encode(voucher.signature),
            }))
        }
        Err(voucher_err) => {
            METRICS.collect_receipts_failed.inc();
            tracing::info!(%voucher_err);
            HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(voucher_err.to_string())
        }
    }
}

#[derive(Clone)]
struct NetworkSubgraphQueryData {
    http_client: reqwest::Client,
    network_subgraph: String,
    network_subgraph_auth_token: String,
}

#[tracing::instrument(skip(payload, data))]
async fn handle_network_query(
    _: HttpRequest,
    payload: String,
    data: web::Data<NetworkSubgraphQueryData>,
) -> HttpResponse {
    let _timer = METRICS.network_subgraph_queries_duration.start_timer();
    let post_request = |body: String| async {
        let response = data
            .http_client
            .post(&data.network_subgraph)
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
            METRICS.network_subgraph_queries_ok.inc();
            HttpResponseBuilder::new(StatusCode::OK).body(result)
        }
        Err(network_subgraph_post_err) => {
            tracing::error!(%network_subgraph_post_err);
            METRICS.network_subgraph_queries_failed.inc();
            graphql_error_response(StatusCode::OK, "Failed to process network subgraph query")
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
    block_resolvers: Arc<HashMap<String, BlockResolver>>,
    subgraph_info: SubgraphInfoMap,
    inputs: Inputs,
    api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    stats_db: mpsc::UnboundedSender<stats_db::Msg>,
}

#[tracing::instrument(skip(request, payload, data))]
async fn handle_subgraph_query(
    request: HttpRequest,
    payload: web::Json<QueryBody>,
    data: web::Data<SubgraphQueryData>,
) -> HttpResponse {
    let t0 = Instant::now();
    let query_id = QueryID::new();
    let response = handle_subgraph_query_inner(request, payload, data, query_id).await;
    let response_time = Instant::now() - t0;
    let (payload, status) = match response {
        Ok(payload) => {
            let status = payload.status().to_string();
            (payload, status)
        }
        Err((status, msg)) => (graphql_error_response(status, msg), msg.to_string()),
    };
    tracing::info!(
        %query_id,
        %status,
        response_time_ms = response_time.as_millis() as u32,
        "client query result",
    );
    payload
}

#[tracing::instrument(skip(request, payload, data))]
async fn handle_subgraph_query_inner(
    request: HttpRequest,
    payload: web::Json<QueryBody>,
    data: web::Data<SubgraphQueryData>,
    query_id: QueryID,
) -> Result<HttpResponse, (StatusCode, &'static str)> {
    let query_engine = QueryEngine::new(
        data.config.clone(),
        data.indexer_client.clone(),
        data.block_resolvers.clone(),
        data.subgraph_info.clone(),
        data.inputs.clone(),
    );
    let url_params = request.match_info();
    let subgraph = if let Some(id) = url_params
        .get("subgraph_id")
        .and_then(|id| id.parse::<SubgraphID>().ok())
    {
        Subgraph::ID(id)
    } else if let Some(deployment) = url_params
        .get("deployment_id")
        .and_then(|id| SubgraphDeploymentID::from_ipfs_hash(&id))
    {
        Subgraph::Deployment(deployment)
    } else {
        return Err((StatusCode::BAD_REQUEST, "Invalid subgraph identifier"));
    };
    let api_keys = data.api_keys.value_immediate().unwrap_or_default();
    let api_key = match url_params.get("api_key").and_then(|k| api_keys.get(k)) {
        Some(api_key) => api_key.clone(),
        None => {
            METRICS.unknown_api_key.inc();
            return Err((StatusCode::BAD_REQUEST, "Invalid API key"));
        }
    };
    if !api_key.queries_activated {
        return Err((
            StatusCode::OK,
            "Querying not activated yet; make sure to add some GRT to your balance in the studio",
        ));
    }
    let domain = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Some(v.parse::<Url>().ok()?.host_str()?.to_string()))
        .unwrap_or("".to_string());
    tracing::debug!(%domain, authorized = ?api_key.domains);
    if !api_key.domains.is_empty()
        && !api_key
            .domains
            .iter()
            .any(|(authorized, _)| domain.starts_with(authorized))
    {
        with_metric(&METRICS.unauthorized_domain, &[&api_key.key], |c| c.inc());
        return Err((StatusCode::OK, "Domain not authorized by API key"));
    }

    let query = ClientQuery {
        id: query_id,
        api_key: api_key.clone(),
        query: payload.query.clone(),
        variables: payload.variables.as_ref().map(ToString::to_string),
        network: data.config.network.clone(),
        subgraph: subgraph.clone(),
    };
    let result = match query_engine.execute_query(query).await {
        Ok(result) => result,
        Err(err) => {
            return Err((
                StatusCode::OK,
                match err {
                    QueryEngineError::MalformedQuery => "Invalid query",
                    QueryEngineError::SubgraphNotFound => "Subgraph deployment not found",
                    QueryEngineError::NoIndexers => "No indexers found for subgraph deployment",
                    QueryEngineError::NoIndexerSelected => {
                        "No suitable indexer found for subgraph deployment"
                    }
                    QueryEngineError::APIKeySubgraphNotAuthorized => {
                        "Subgraph not authorized by API key"
                    }
                    QueryEngineError::MissingBlock(_) => {
                        "Gateway failed to resolve required blocks"
                    }
                },
            ))
        }
    };
    if let Ok(hist) = METRICS
        .query_result_size
        .get_metric_with_label_values(&[&result.query.indexing.deployment.ipfs_hash()])
    {
        hist.observe(result.response.payload.len() as f64);
    }
    let _ = data.stats_db.send(stats_db::Msg::AddQuery {
        api_key,
        fee: result.query.fee,
        domain: domain.to_string(),
        subgraph: match subgraph {
            Subgraph::ID(id) => Some(id.to_string()),
            Subgraph::Deployment(_) => None,
        },
    });
    Ok(HttpResponseBuilder::new(StatusCode::OK)
        .insert_header(header::ContentType::json())
        .body(result.response.payload))
}

pub fn graphql_error_response<S: ToString>(status: StatusCode, message: S) -> HttpResponse {
    HttpResponseBuilder::new(status)
        .insert_header(header::ContentType::json())
        .body(json!({"errors": {"message": message.to_string()}}).to_string())
}

#[derive(Clone)]
struct Metrics {
    collect_receipts_duration: prometheus::Histogram,
    collect_receipts_failed: prometheus::IntCounter,
    collect_receipts_ok: prometheus::IntCounter,
    network_subgraph_queries_duration: prometheus::Histogram,
    network_subgraph_queries_failed: prometheus::IntCounter,
    network_subgraph_queries_ok: prometheus::IntCounter,
    query_result_size: prometheus::HistogramVec,
    unauthorized_domain: prometheus::IntCounterVec,
    unknown_api_key: prometheus::IntCounter,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
}

impl Metrics {
    fn new() -> Self {
        Self {
            collect_receipts_duration: prometheus::register_histogram!(
                "gateway_collect_receipts_duration",
                "Duration of processing requests to collect receipts"
            )
            .unwrap(),
            // TODO: should be renamed to gateway_collect_receipt_requests_failed
            collect_receipts_failed: prometheus::register_int_counter!(
                "gateway_failed_collect_receipt_requests",
                "Failed requests to collect receipts"
            )
            .unwrap(),
            // TODO: should be renamed to gateway_collect_receipt_requests_ok
            collect_receipts_ok: prometheus::register_int_counter!(
                "gateway_collect_receipt_requests",
                "Incoming requests to collect receipts"
            )
            .unwrap(),
            network_subgraph_queries_duration: prometheus::register_histogram!(
                "gateway_network_subgraph_query_duration",
                "Duration of processing a network subgraph query"
            )
            .unwrap(),
            network_subgraph_queries_failed: prometheus::register_int_counter!(
                "gateway_network_subgraph_queries_failed",
                "Network subgraph queries that failed executing"
            )
            .unwrap(),
            network_subgraph_queries_ok: prometheus::register_int_counter!(
                "gateway_network_subgraph_queries_ok",
                "Successfully executed network subgraph queries"
            )
            .unwrap(),
            query_result_size: prometheus::register_histogram_vec!(
                "query_engine_query_result_size",
                "Size of query result",
                &["deployment"]
            )
            .unwrap(),
            unauthorized_domain: prometheus::register_int_counter_vec!(
                "gateway_queries_from_unauthorized_domain",
                "Queries from a domain not authorized in the API key",
                &["apiKey"],
            )
            .unwrap(),
            unknown_api_key: prometheus::register_int_counter!(
                "gateway_queries_for_unknown_api_key",
                "Queries made against an unknown API key",
            )
            .unwrap(),
        }
    }
}
