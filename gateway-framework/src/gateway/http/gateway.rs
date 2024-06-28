use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Debug,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{Address, BlockNumber, U256};
use alloy_sol_types::Eip712Domain;
use axum::{
    async_trait,
    body::{Body, Bytes},
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, HeaderMap, Request, Uri},
    middleware,
    response::Response,
    routing, Router,
};
use eventuals::{Eventual, EventualExt, Ptr};
use ordered_float::NotNan;
use prometheus::Encoder;
use reqwest::StatusCode;
use secp256k1::SecretKey;
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use thegraph_core::{
    client as subgraph_client,
    types::{attestation, Attestation, DeploymentId, SubgraphId},
};
use tokio::{
    net::TcpListener,
    signal::unix::SignalKind,
    spawn,
    sync::{mpsc, watch},
    time::{interval, MissedTickBehavior},
};
use tower_http::cors::{self, CorsLayer};
use url::Url;

use super::requests::blocks::BlockRequirements;
use crate::{
    auth::{api_keys::APIKey, AuthContext},
    blocks::{Block, BlockConstraint},
    budgets::{Budgeter, USD},
    chain::Chain,
    chains::Chains,
    config::{ApiKeys, ExchangeRateProvider, GatewayConfig},
    errors::{Error, IndexerError},
    gateway::http::requests::handler::{GatewayRequestContext, IncomingRequest},
    http::middleware::{
        legacy_auth_adapter, RequestTracingLayer, RequireAuthorizationLayer, SetRequestIdLayer,
    },
    indexing::Indexing,
    ip_blocker::IpBlocker,
    json, logging,
    network::{
        exchange_rate,
        indexing_performance::{IndexingPerformance, Status as IndexingPerformanceStatus},
        network_subgraph::Subgraph,
    },
    reports,
    scalar::{self, ReceiptSigner, ScalarReceipt},
    topology::{
        keep_allocations_up_to_date,
        network::{Deployment, GraphNetwork, Indexer},
    },
};

#[derive(Clone, Debug)]
pub struct DeterministicRequest {
    pub body: String,
    pub headers: HeaderMap,
}

#[derive(Clone)]
pub struct IndexerResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub attestation: Option<Attestation>,
    pub original_response: String,
    pub client_response: String,
    pub errors: Vec<String>,
    pub probe_block: Option<Block>,
}

pub struct BlockError {
    pub unresolved: Option<BlockNumber>,
    pub latest_block: Option<BlockNumber>,
}

pub trait IndexerResponseError {
    fn message(&self) -> &str;
}

pub trait DetailedIndexerResponse {
    type Error: IndexerResponseError;

    fn errors(&self) -> &[Self::Error];
    fn block(&self) -> Option<Block>;
}

pub trait IndexingStatus: Send + Sync + 'static {
    fn block(&self) -> BlockNumber;
    fn min_block(&self) -> Option<BlockNumber>;
    fn legacy_scalar(&self) -> bool;
}

#[async_trait]
pub trait GatewayImpl: Send + Sync + 'static {
    type Request: Debug + Send;
    type IndexingStatus: IndexingStatus;

    /// Provide a live view of all datasets for the data service in question.
    /// This could be a list of subgraphs, a list of substreams, each with all
    /// versions and deployments that were published.
    ///
    /// In most gateway implementations, this would query the network subgraph
    /// to get all datasets for the data service, along with their versions,
    /// deployments and so on.
    async fn datasets(
        &self,
        network_subgraph: subgraph_client::Client,
        l2_transfer_support: bool,
    ) -> Eventual<Ptr<Vec<Subgraph>>>;

    /// Provide a live view of all indexer statuses for a given set of deployments.
    ///
    /// In most gateway implementations, this would query the network subgraph
    /// to find all indexers that have allocated towards one of the deployments
    /// and then fetching the indexing status for the matching deployments from
    /// each indexer.
    async fn indexing_statuses(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    ) -> Eventual<Ptr<HashMap<Indexing, Self::IndexingStatus>>>;

    /// Provide a live view of all indexers to exclude from serving requests.
    ///
    /// The live view allows for both a static blocklist (e.g. via the gateway
    /// configuration file) as well as a dynamically changing blocklist behind
    /// the scenes.
    async fn indexings_blocklist(
        &self,
        _deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
        _indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
    ) -> Eventual<Ptr<HashSet<Indexing>>> {
        Eventual::from_value(Ptr::new(HashSet::new()))
    }

    /// Provide a live view of all API keys the gateway knows.
    ///
    /// This allows for different API key solutions to be built, as long as each
    /// key supports some common pieces of information, such as
    ///
    /// - the key itself,
    /// - the on-chain address of the user that owns the API key,
    /// - a status (active, cut off, monthly cap reached),
    /// - a maximum budget for individual requests,
    /// - a list of datasets to restrict the API key to, and
    /// - a list of origin domains to restrict the API key to,
    ///
    /// As long as this information is provided for every API key, the API keys
    /// can come from anything, any type of API, any sort of studio software.
    async fn api_keys(&self) -> watch::Receiver<HashMap<String, APIKey>>;

    /// Forward requests to an L2 gateway (optional).
    ///
    /// This only exists for the subgraph gateway, which has historically been
    /// running in two configurations: initially L1 only, then L1 and L2.  All
    /// future gateways will likely run against L2 only.
    async fn forward_request_to_l2(
        &self,
        _l2_url: &Url,
        _original_path: &Uri,
        _headers: HeaderMap,
        _body: Bytes,
        _l2_subgraph_id: Option<SubgraphId>,
    ) -> Response<String> {
        unimplemented!("this gateway does not support forwarding requests to an L2 gateway");
    }

    /// Parse an incoming request into whatever the gateway needs to process
    /// the request further down the line.
    ///
    /// Example: The subgraph gateway creates an Agora cost model context for
    /// the request, which involves interpreting the request as JSON and parsing
    /// the GraphQL query and variables.
    async fn parse_request(
        &self,
        context: &GatewayRequestContext,
        request: &IncomingRequest,
    ) -> Result<Self::Request, Error>;

    /// Extract all block constraints present in the request.
    ///
    /// Block constraints are any specific blocks (by number or hash) that the
    /// request is related to, or range of blocks greater than a given block
    /// number.
    async fn block_constraints(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
    ) -> Result<BTreeSet<BlockConstraint>, Error>;

    /// Calculate the fee a given indexer is charging for a request.
    ///
    /// The subgraph gateway uses Agora to calculate the cost of a query, but
    /// gateways are free to calculate the request fees in their own ways,
    /// typically based on the cost model provided by the indexer.
    fn indexer_request_fee(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
        status: &Self::IndexingStatus,
    ) -> Result<u128, IndexerError>;

    /// Make a request deterministic, typically by replacing block numbers with
    /// block hashes to avoid any ambiguity in requests sent to an indexer.
    fn deterministic_request(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
        chain: &Chain,
        block_requirements: &BlockRequirements,
        blocks_behind: u64,
    ) -> DeterministicRequest;

    /// Send a request to an indexer.
    async fn send_request_to_indexer(
        &self,
        context: &GatewayRequestContext,
        deployment: &DeploymentId,
        url: &Url,
        receipt: &ScalarReceipt,
        attestation_domain: &Eip712Domain,
        request: DeterministicRequest,
    ) -> Result<IndexerResponse, IndexerError>;

    /// Prepare the response for being sent back to the client.
    ///
    /// This allows to convert between different response formats, modify the
    /// response headers and put the attestation in the right place in the
    /// client response.
    async fn finalize_response(&self, result: Result<IndexerResponse, Error>) -> Response<String>;
}

pub struct GatewayState<G>
where
    G: GatewayImpl,
{
    pub config: GatewayConfig,
    pub gateway_impl: G,

    pub l2_gateway: Option<Url>,
    pub network: GraphNetwork,
    pub chains: &'static Chains,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
    pub bad_indexers: &'static HashSet<Address>,
    pub grt_per_usd: watch::Receiver<NotNan<f64>>,
    pub budgeter: &'static Budgeter,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, G::IndexingStatus>>>,
    pub indexing_performance: IndexingPerformance,
    pub receipt_signer: &'static ReceiptSigner,
    pub attestation_domain: &'static Eip712Domain,
    pub reporter: mpsc::UnboundedSender<reports::ClientRequest>,
}

pub struct GatewayOptions<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    pub config: GatewayConfig,
    pub gateway_impl: G,
    pub api: Router<Arc<GatewayState<G>>>,
}

pub struct Gateway<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    pub state: Arc<GatewayState<G>>,
}

impl<G> Gateway<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    pub async fn run(options: GatewayOptions<G>) -> Result<(), anyhow::Error> {
        let GatewayOptions {
            config,
            gateway_impl,
            api,
        } = options;

        let gateway_config = config.clone();

        let GatewayConfig {
            chains,
            gateway_details,
            indexer_selection,
            network,
            payments,
            ..
        } = config;

        let network_id = network.id.clone();

        // Initialize logging
        logging::init(gateway_details.executable_name, config.log_json);
        tracing::info!("Gateway starting... ID: {}", gateway_details.id);

        let ip_blocker = IpBlocker::new(indexer_selection.ip_blocker_db.as_deref()).unwrap();
        let chains = Box::leak(Box::new(Chains::new(chains.aliases)));

        let grt_per_usd: watch::Receiver<NotNan<f64>> = match payments.exchange_rate_provider {
            ExchangeRateProvider::Fixed(grt_per_usd) => {
                watch::channel(NotNan::new(grt_per_usd).expect("NAN exchange rate")).1
            }
            ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await.unwrap(),
        };

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        let api_keys = gateway_impl.api_keys().await;
        let auth_context = AuthContext {
            payment_required: payments.payment_required,
            api_keys,
            special_api_keys: match &config.api_keys {
                Some(ApiKeys::Endpoint { special, .. }) => {
                    Arc::new(HashSet::from_iter(special.clone()))
                }
                _ => Default::default(),
            },
        };

        let network_subgraph_client =
            subgraph_client::Client::new(http_client.clone(), network.network_subgraph);
        let subgraphs = gateway_impl
            .datasets(
                network_subgraph_client,
                gateway_details.l2_gateway.is_some(),
            )
            .await;

        let attestation_domain: &'static Eip712Domain =
            Box::leak(Box::new(attestation::eip712_domain(
                U256::from_str_radix(&network.attestations.chain_id, 10)
                    .expect("failed to parse attestation domain chain_id"),
                network.attestations.dispute_manager,
            )));

        let network = GraphNetwork::new(subgraphs, ip_blocker).await;

        let bad_indexers: &'static HashSet<Address> = Box::leak(Box::new(
            indexer_selection.bad_indexers.into_iter().collect(),
        ));

        let indexing_statuses = gateway_impl
            .indexing_statuses(network.deployments.clone())
            .await;
        let latest_indexed_block_statuses = indexing_statuses.clone().map(|statuses| async move {
            let statuses = statuses
                .iter()
                .map(|(id, status)| {
                    (
                        (id.indexer, id.deployment),
                        IndexingPerformanceStatus {
                            latest_block: status.block(),
                        },
                    )
                })
                .collect::<HashMap<_, _>>();
            Ptr::new(statuses)
        });

        let indexings_blocklist = gateway_impl
            .indexings_blocklist(network.deployments.clone(), network.indexers.clone())
            .await;

        let legacy_signer: &'static SecretKey = Box::leak(Box::new(
            payments
                .scalar
                .legacy_signer
                .map(|s| s.0)
                .unwrap_or(payments.scalar.signer.0),
        ));
        let receipt_signer: &'static ReceiptSigner = Box::leak(Box::new(
            ReceiptSigner::new(
                payments.scalar.signer.0,
                payments.scalar.chain_id,
                payments.scalar.verifier,
                legacy_signer,
            )
            .await,
        ));

        keep_allocations_up_to_date(
            receipt_signer,
            network.deployments.clone(),
            indexing_statuses.clone(),
        );

        let query_fees_target =
            USD(NotNan::new(payments.query_fees_target).expect("invalid query_fees_target"));
        let budgeter: &'static Budgeter = Box::leak(Box::new(Budgeter::new(query_fees_target)));

        let reporter = reports::Reporter::create(
            network_id.clone(),
            "gateway_client_request_results".into(),
            "gateway_indexer_attempts".into(),
            "gateway_attestations".into(),
            &config.kafka.into(),
        )
        .unwrap();

        // Host metrics on a separate server with a port that isn't open to
        // public requests
        spawn(async move {
            let router = Router::new().route("/metrics", routing::get(handle_metrics));

            let listener = TcpListener::bind(&SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                config.metrics_port,
            ))
            .await
            .expect("Failed to bind metrics server");

            axum::serve(listener, router.into_make_service())
                // disable Nagel's algorithm
                .tcp_nodelay(true)
                .await
                .expect("Failed to start metrics server");
        });

        let state = Arc::new(GatewayState {
            config: gateway_config,
            gateway_impl,

            l2_gateway: gateway_details.l2_gateway,
            network,
            chains,
            indexings_blocklist,
            bad_indexers,
            grt_per_usd,
            budgeter,
            indexing_performance: IndexingPerformance::new(latest_indexed_block_statuses),
            indexing_statuses,
            receipt_signer,
            attestation_domain,
            reporter,
        });

        let common_routes = Router::new()
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
            );

        let api_routes = api
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
                    .layer(RequestTracingLayer)
                    // Set the request ID on the request
                    .layer(SetRequestIdLayer::new(gateway_details.id))
                    // Handle legacy in-path auth, and convert it into a header
                    .layer(middleware::from_fn(legacy_auth_adapter))
                    // Require requests to be authorized
                    .layer(RequireAuthorizationLayer::new(auth_context)),
            )
            .with_state(state.clone());

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

        let router = common_routes
            .with_state(state)
            .nest("/api", api_routes)
            .layer(middleware::from_fn_with_state(rate_limiter, ip_rate_limit));

        let listener = TcpListener::bind(&SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            config.api_port,
        ))
        .await
        .expect("Failed to bind API server");

        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        // disable Nagel's algorithm
        .tcp_nodelay(true)
        .with_graceful_shutdown(await_shutdown_signals())
        .await
        .expect("Failed to start API server");

        tracing::warn!("shutdown");

        Ok(())
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

pub async fn ip_rate_limit(
    State(limiter): State<&'static RateLimiter<String>>,
    ConnectInfo(info): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: middleware::Next,
) -> Result<Response, json::JsonResponse> {
    if limiter.check_limited(info.ip().to_string()) {
        return Err(graphql_error_response("Too many requests, try again later"));
    }
    Ok(next.run(req).await)
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

fn graphql_error_response<S: ToString>(message: S) -> json::JsonResponse {
    json::json_response([], json!({"errors": [{"message": message.to_string()}]}))
}
