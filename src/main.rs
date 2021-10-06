mod alchemy_client;
mod indexer_selection;
mod prelude;
mod query_engine;
mod rate_limiter;
mod sync_client;
mod ws_client;

use crate::{indexer_selection::SecretKey, prelude::*, query_engine::*, rate_limiter::*};
use actix_web::{
    dev::ServiceRequest,
    http::{header, StatusCode},
    web, App, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer,
};
use async_trait::async_trait;
use bip39;
use eventuals::EventualExt;
use graphql_client::GraphQLQuery;
use hdwallet::{self, KeyChain as _};
use hex;
use indexer_selection::{IndexerQuery, UnresolvedBlock, UtilityConfig};
use lazy_static::lazy_static;
use prometheus::{self, Encoder as _};
use reqwest;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use std::{
    collections::HashMap,
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
};
use structopt::StructOpt;
use structopt_derive::StructOpt;
use tokio::time::Duration;
use tree_buf;
use uuid::Uuid;

#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(
        help = "Ethereum wallet mnemonic",
        long = "--mnemonic",
        env = "MNEMONIC"
    )]
    mnemonic: String,
    #[structopt(
        help = "URL of gateway agent syncing API",
        long = "--sync-agent",
        env = "SYNC_AGENT"
    )]
    sync_agent: String,
    #[structopt(
        help = "Ethereum provider URLs, format: '<network>=<url>,...'\ne.g. rinkeby=eth-rinkeby.alchemyapi.io/v2/<api-key>",
        long = "--ethereum-providers",
        env = "ETHEREUM_PROVIDERS"
    )]
    ethereum_proviers: EthereumProviders,
    #[structopt(
        help = "Network subgraph URL",
        long = "--network-subgraph",
        env = "NETWORK_SUBGRAPH"
    )]
    network_subgraph: String,
    #[structopt(help = "Format log output as JSON", long = "--log-json")]
    log_json: bool,
    #[structopt(
        long = "--indexer-selection-retry-limit",
        env = "INDEXER_SELECTION_LIMIT",
        default_value = "5"
    )]
    indexer_selection_retry_limit: usize,
    #[structopt(
        long = "--query-budget",
        env = "QUERY_BUDGET",
        default_value = "0.0005"
    )]
    query_budget: GRT,
    #[structopt(long = "--port", env = "PORT", default_value = "6700")]
    port: u16,
    #[structopt(long = "--metrics-port", env = "METRICS_PORT", default_value = "7300")]
    metrics_port: u16,
    #[structopt(
        help = "Duration of IP rate limiting window in seconds",
        long = "--ip-rate-limit-window",
        env = "IP_RATE_LIMIT_WINDOW",
        default_value = "10"
    )]
    ip_rate_limit_window_secs: u8,
    #[structopt(
        help = "IP rate limit per window",
        long = "--ip-rate-limit",
        env = "IP_RATE_LIMIT",
        default_value = "250"
    )]
    ip_rate_limit: u16,
    #[structopt(
        help = "Duration of API rate limiting window in seconds",
        long = "--api-rate-limit-window",
        env = "API_RATE_LIMIT_WINDOW",
        default_value = "10"
    )]
    api_rate_limit_window_secs: u8,
    #[structopt(
        help = "API rate limit per window",
        long = "--api-rate-limit",
        env = "API_RATE_LIMIT",
        default_value = "1000"
    )]
    api_rate_limit: u16,
}

#[derive(Debug)]
struct EthereumProviders(Vec<(String, String)>);

impl FromStr for EthereumProviders {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err_usage = "networks syntax: <network>=<url>,...";
        let pairs = s.split(",").collect::<Vec<&str>>();
        if pairs.is_empty() {
            return Err(err_usage.into());
        }
        pairs
            .into_iter()
            .map(|provider| {
                let kv = provider.split("=").collect::<Vec<&str>>();
                if kv.len() != 2 {
                    return None;
                }
                Some((kv[0].into(), kv[1].into()))
            })
            .collect::<Option<Vec<(String, String)>>>()
            .map(|providers| EthereumProviders(providers))
            .ok_or::<String>(err_usage.into())
    }
}

#[actix_web::main]
async fn main() {
    let opt = Opt::from_args();
    init_tracing(opt.log_json);
    tracing::info!("Graph gateway starting...");
    tracing::trace!("{:#?}", opt);
    let gateway_id = Uuid::new_v4();
    tracing::info!(%gateway_id);

    let wallet_seed = bip39::Seed::new(
        &bip39::Mnemonic::from_phrase(&opt.mnemonic, bip39::Language::English)
            .expect("Invalid mnemonic"),
        "",
    );
    let signer_key = hdwallet::DefaultKeyChain::new(
        hdwallet::ExtendedPrivKey::with_seed(wallet_seed.as_bytes()).expect("Invalid mnemonic"),
    )
    .derive_private_key(key_path("scalar/allocations").into())
    .expect("Failed to derive signer key")
    .0
    .private_key;
    // Convert between versions of secp256k1 lib.
    let signer_key = SecretKey::from_slice(signer_key.as_ref()).unwrap();
    // Zeroize the wallet seed.
    drop(wallet_seed);

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

    let (block_resolvers, block_metrics): (
        HashMap<String, mpsc::Sender<alchemy_client::Request>>,
        Vec<alchemy_client::Metrics>,
    ) = opt
        .ethereum_proviers
        .0
        .into_iter()
        .map(|(network, url)| {
            let (send, metrics) =
                alchemy_client::create(network.clone(), url, input_writers.indexers.clone());
            ((network, send), metrics)
        })
        .unzip();
    let (api_keys_writer, api_keys) = Eventual::new();
    let sync_metrics = sync_client::create(
        opt.sync_agent,
        Duration::from_secs(30),
        gateway_id,
        signer_key.clone(),
        input_writers,
        api_keys_writer,
    );
    let config = query_engine::Config {
        indexer_selection_retry_limit: opt.indexer_selection_retry_limit,
        utility: UtilityConfig::default(),
        query_budget: opt.query_budget,
    };
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    // TODO: argument for timeout
    let resolver = NetworkResolver {
        block_resolvers: Arc::new(block_resolvers),
        client: http_client.clone(),
        network_subgraph_url: opt.network_subgraph.clone(),
        gateway_id,
    };
    static QUERY_ID: AtomicUsize = AtomicUsize::new(0);
    let network_subgraph = opt.network_subgraph;
    let metrics_port = opt.metrics_port;
    let indexer_selection = inputs.indexers.clone();
    // Host metrics on a separate server with a port that isn't open to public requests.
    actix_web::rt::spawn(async move {
        HttpServer::new(move || {
            App::new()
                .route("/metrics", web::get().to(handle_metrics))
                .service(
                    web::resource("/snapshot")
                        .app_data(web::Data::new(indexer_selection.clone()))
                        .route(web::get().to(handle_snapshot)),
                )
        })
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
        let api = web::scope("/api/{api_key}")
            .wrap(RateLimiterMiddleware {
                rate_limiter: api_rate_limiter.clone(),
                key: request_api_key,
            })
            .app_data(web::Data::new(SubgraphQueryData {
                config: config.clone(),
                resolver: resolver.clone(),
                inputs: inputs.clone(),
                api_keys: api_keys.clone(),
                query_id: &QUERY_ID,
            }))
            .route(
                "/subgraphs/id/{subgraph_id}",
                web::post().to(handle_subgraph_query),
            )
            .route(
                "/deployments/id/{deployment_id}",
                web::post().to(handle_subgraph_query),
            );
        let other = web::scope("/")
            .wrap(RateLimiterMiddleware {
                rate_limiter: ip_rate_limiter.clone(),
                key: request_host,
            })
            .route("/", web::get().to(|| async { "Ready to roll!" }))
            .service(
                web::resource("/ready")
                    .app_data(web::Data::new((
                        block_metrics.clone(),
                        sync_metrics.clone(),
                    )))
                    .route(web::get().to(handle_ready)),
            )
            .service(
                web::resource("/network")
                    .app_data(web::Data::new((
                        http_client.clone(),
                        network_subgraph.clone(),
                    )))
                    .route(web::post().to(handle_network_query)),
            )
            .service(
                web::resource("/collect-receipts")
                    .app_data(web::PayloadConfig::new(16_000_000))
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
async fn handle_snapshot(data: web::Data<Arc<indexer_selection::Indexers>>) -> HttpResponse {
    let snapshot = data.snapshot().await;
    tracing::trace!(?snapshot);
    let encoded = tree_buf::encode(&snapshot);
    tracing::info!(snapshot_size = %encoded.len());
    HttpResponseBuilder::new(StatusCode::OK)
        .insert_header(header::ContentType::octet_stream())
        .body(encoded)
}

#[tracing::instrument(skip(data))]
async fn handle_ready(
    data: web::Data<(Vec<alchemy_client::Metrics>, sync_client::Metrics)>,
) -> HttpResponse {
    let ready = data.0.iter().all(|metrics| metrics.head_block.get() > 0)
        && (data.1.allocations.get() > 0)
        && (data.1.transfers.get() > 0);
    if ready {
        HttpResponseBuilder::new(StatusCode::OK).body("Ready")
    } else {
        // Respond with 425 Too Early
        HttpResponseBuilder::new(StatusCode::from_u16(425).unwrap()).body("Not ready")
    }
}

#[tracing::instrument(skip(payload))]
async fn handle_collect_receipts(data: web::Data<SecretKey>, payload: String) -> HttpResponse {
    let _timer = METRICS.collect_receipts_duration.start_timer();
    let bytes = payload.into_bytes();
    if bytes.len() < 20 {
        return HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body("Invalid receipt data");
    }
    let mut allocation_id = [0u8; 20];
    allocation_id.copy_from_slice(&bytes[..20]);
    let result = indexer_selection::Receipts::receipts_to_voucher(
        &allocation_id.into(),
        data.as_ref(),
        &bytes[20..],
    );
    match result {
        Ok(voucher) => {
            METRICS.collect_receipts_ok.inc();
            tracing::info!(request_size = %bytes.len(), "Collect receipts");
            HttpResponseBuilder::new(StatusCode::OK).json(json!({
                "allocation_id": voucher.allocation_id,
                "fees": voucher.fees.to_string(),
                "signature": format!("0x{}", hex::encode(voucher.signature)),
            }))
        }
        Err(voucher_err) => {
            METRICS.collect_receipts_failed.inc();
            tracing::info!(%voucher_err);
            HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(voucher_err.to_string())
        }
    }
}

#[tracing::instrument(skip(payload, data))]
async fn handle_network_query(
    _: HttpRequest,
    payload: String,
    data: web::Data<(reqwest::Client, String)>,
) -> HttpResponse {
    let _timer = METRICS.network_subgraph_queries_duration.start_timer();
    let post_request = |body: String| async {
        let response = data
            .0
            .post(&data.1)
            .body(body)
            .header(header::CONTENT_TYPE.as_str(), "application/json")
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

struct SubgraphQueryData {
    config: Config,
    resolver: NetworkResolver,
    inputs: Inputs,
    api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    query_id: &'static AtomicUsize,
}

#[tracing::instrument(skip(request, payload, data))]
async fn handle_subgraph_query(
    request: HttpRequest,
    payload: web::Json<QueryBody>,
    data: web::Data<SubgraphQueryData>,
) -> HttpResponse {
    let query_engine = QueryEngine::new(
        data.config.clone(),
        data.resolver.clone(),
        data.inputs.clone(),
    );
    let url_params = request.match_info();
    let subgraph = if let Some(name) = url_params.get("subgraph_id") {
        Subgraph::Name(name.into())
    } else if let Some(deployment) = url_params
        .get("deployment_id")
        .and_then(|id| SubgraphDeploymentID::from_ipfs_hash(&id))
    {
        Subgraph::Deployment(deployment)
    } else {
        return graphql_error_response(StatusCode::BAD_REQUEST, "Invalid subgraph identifier");
    };
    let api_keys = data.api_keys.value_immediate().unwrap_or_default();
    let api_key = match url_params.get("api_key").and_then(|k| api_keys.get(k)) {
        Some(api_key) => api_key.clone(),
        None => return graphql_error_response(StatusCode::BAD_REQUEST, "Invalid API key"),
    };
    let connection_info = request.connection_info();
    let host = connection_info.host();
    if !api_key.domains.is_empty()
        && !api_key
            .domains
            .iter()
            .any(|domain| host.starts_with(domain))
    {
        return graphql_error_response(StatusCode::OK, "Domain not authorized by API key");
    }

    let query = ClientQuery {
        id: data.query_id.fetch_add(1, MemoryOrdering::Relaxed) as u64,
        api_key,
        query: payload.query.clone(),
        variables: payload.variables.as_ref().map(ToString::to_string),
        // TODO: We are assuming mainnet for now.
        network: "mainnet".into(),
        subgraph,
    };
    let (query, body) = match query_engine.execute_query(query).await {
        Ok(result) => match serde_json::to_string(&result.response.graphql_response) {
            Ok(body) => (result.query, body),
            Err(err) => return graphql_error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => return graphql_error_response(StatusCode::OK, format!("{:?}", err)),
    };
    if let Ok(hist) = METRICS
        .query_result_size
        .get_metric_with_label_values(&[&query.indexing.subgraph.ipfs_hash()])
    {
        hist.observe(body.len() as f64);
    }
    HttpResponseBuilder::new(StatusCode::OK)
        .insert_header(header::ContentType::json())
        .body(body)
}

pub fn graphql_error_response<S: ToString>(status: StatusCode, message: S) -> HttpResponse {
    HttpResponseBuilder::new(status)
        .insert_header(header::ContentType::json())
        .body(json!({"errors": {"message": message.to_string()}}).to_string())
}

#[derive(Clone)]
struct NetworkResolver {
    block_resolvers: Arc<HashMap<String, mpsc::Sender<alchemy_client::Request>>>,
    client: reqwest::Client,
    network_subgraph_url: String,
    gateway_id: Uuid,
}

#[async_trait]
impl Resolver for NetworkResolver {
    #[tracing::instrument(skip(self, network, unresolved))]
    async fn resolve_blocks(
        &self,
        network: &str,
        unresolved: &[UnresolvedBlock],
    ) -> Vec<BlockHead> {
        use alchemy_client::Request;
        let mut resolved_blocks = Vec::new();
        let resolver = match self.block_resolvers.get(network) {
            Some(resolver) => resolver,
            None => {
                tracing::error!(missing_network = network);
                return resolved_blocks;
            }
        };
        for unresolved_block in unresolved {
            let (sender, receiver) = oneshot::channel();
            if let Err(_) = resolver
                .send(Request::Block(unresolved_block.clone(), sender))
                .await
            {
                tracing::error!("block resolver connection closed");
                return resolved_blocks;
            }
            match receiver.await {
                Ok(resolved) => resolved_blocks.push(resolved),
                Err(_) => {
                    tracing::error!("block resolver connection closed");
                    return resolved_blocks;
                }
            };
        }
        resolved_blocks
    }

    async fn query_indexer(&self, query: &IndexerQuery) -> Result<IndexerResponse, Box<dyn Error>> {
        let receipt = hex::encode(&query.receipt.commitment);
        let receipt = &receipt[0..(receipt.len() - 64)];
        self.client
            .post(format!(
                "{}/subgraphs/id/{:?}",
                query.url, query.indexing.subgraph
            ))
            .header("Scalar-Receipt", receipt)
            .body(query.query.clone())
            .send()
            .await?
            .json::<IndexerResponse>()
            .await
            .map_err(|err| err.into())
    }

    #[tracing::instrument(skip(self, indexers, indexing, fee))]
    async fn create_transfer(
        &self,
        indexers: &indexer_selection::Indexers,
        indexing: Indexing,
        fee: GRT,
    ) -> Result<(), Box<dyn Error>> {
        // TODO: We need to limit the total number of transfers to 2, if/when we can potentially
        // create multiple transfers.
        tracing::info!(
            deployment = ?indexing.subgraph,
            indexer = ?indexing.indexer,
            fee = ?fee,
            "Creating transfer to increase collateral",
        );
        let query = CreateTransfer::build_query(create_transfer::Variables {
            gateway_id: self.gateway_id.to_string(),
            deployment: indexing.subgraph.ipfs_hash(),
            indexer: indexing.indexer.to_string(),
        });
        let response = self
            .client
            .post(&self.network_subgraph_url)
            .json(&query)
            .send()
            .await?
            .json::<Response<create_transfer::ResponseData>>()
            .await?;
        if let Some(errors) = response.errors {
            return Err(errors
                .into_iter()
                .map(|err| err.message)
                .collect::<Vec<String>>()
                .join(", ")
                .into());
        }
        let transfer = match response.data {
            Some(data) => data.create_transfer,
            None => return Err("Empty transfer data".into()),
        };
        let transfer_id = transfer
            .id
            .parse::<Bytes32>()
            .map_err(|_| "Malformed transfer ID")?;
        let transfer_indexing = Indexing {
            subgraph: transfer
                .deployment
                .parse()
                .map_err(|_| "Malformed transfer deployment ID")?,
            indexer: transfer
                .indexer
                .id
                .parse()
                .map_err(|_| "Malformed transfer indexer ID")?,
        };
        let transfer_collateral = transfer
            .collateral
            .parse::<GRT>()
            .map_err(|_| "Malformed transfer collateral")?;
        let signer_key = transfer
            .signer_key
            .parse::<SecretKey>()
            .map_err(|_| "Malformed signer key")?;
        tracing::trace!(
            id = ?transfer_id,
            deployment = ?transfer_indexing.subgraph,
            indexer = ?transfer_indexing.indexer,
            collateral = ?transfer_collateral,
            "Successfully created transfer to increase collateral",
        );
        indexers
            .install_receipts_transfer(
                &transfer_indexing,
                transfer_id,
                &transfer_collateral,
                signer_key,
            )
            .await;
        Ok(())
    }
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/create_transfer.gql",
    response_derives = "Debug"
)]
struct CreateTransfer;

#[derive(Clone)]
struct Metrics {
    collect_receipts_duration: prometheus::Histogram,
    collect_receipts_failed: prometheus::IntCounter,
    collect_receipts_ok: prometheus::IntCounter,
    network_subgraph_queries_duration: prometheus::Histogram,
    network_subgraph_queries_failed: prometheus::IntCounter,
    network_subgraph_queries_ok: prometheus::IntCounter,
    query_result_size: prometheus::HistogramVec,
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
        }
    }
}
