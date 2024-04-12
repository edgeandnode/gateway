use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{Address, BlockNumber, U256};
use alloy_sol_types::Eip712Domain;
use anyhow::anyhow;
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
use gateway_common::types::Indexing;
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
use tokio::{net::TcpListener, signal::unix::SignalKind, spawn};
use tower_http::cors::{self, CorsLayer};
use url::Url;

use super::{
    config::ExchangeRateProvider, requests::BlockRequirements, GatewayConfig, GatewayRequest,
    GatewayRequestContext,
};
use crate::{
    auth::{context::AuthContext, methods::api_keys::APIKey},
    blocks::BlockConstraint,
    budgets::{Budgeter, USD},
    chain::Chain,
    chains::Chains,
    errors::{Error, IndexerError},
    gateway::http::config::ApiKeys,
    http::middleware::{
        legacy_auth_adapter, AddRateLimiterLayer, RequestTracingLayer, RequireAuthorizationLayer,
        SetRequestIdLayer,
    },
    ip_blocker::IpBlocker,
    json,
    network::{
        exchange_rate, indexing_performance::IndexingPerformance, network_subgraph::Subgraph,
    },
    reporting::{self, EventHandlerFn, KafkaClient, LoggingOptions},
    scalar::{self, ReceiptSigner, ScalarReceipt},
    subscriptions::subgraph as subscriptions_subgraph,
    topology::{
        keep_allocations_up_to_date,
        network::{Deployment, GraphNetwork, Indexer},
    },
};

pub struct IndexerResponse {
    pub status: u16,
    pub body: Body,
    pub headers: HeaderMap,
    pub attestation: Option<Attestation>,
}

pub struct SelectionInfo {
    pub indexing: Indexing,
    pub url: Url,
    pub receipt: ScalarReceipt,
    pub blocks_behind: u64,
}

pub trait IndexingStatus: Send + Sync + 'static {
    fn block(&self) -> BlockNumber;
    fn min_block(&self) -> Option<BlockNumber>;
}

#[async_trait]
pub trait GatewayImpl: Send + Sync + 'static {
    type Request;
    type DeterministicRequest: Send + Sync;
    type IndexingStatus: IndexingStatus;

    async fn publications(
        &self,
        network_subgraph: subgraph_client::Client,
        l2_transfer_support: bool,
    ) -> Eventual<Ptr<Vec<Subgraph>>>;

    async fn indexing_statuses(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    ) -> Eventual<Ptr<HashMap<Indexing, Self::IndexingStatus>>>;

    async fn legacy_indexers(
        &self,
        indexing_statuses: Eventual<Ptr<HashMap<Indexing, Self::IndexingStatus>>>,
    ) -> Eventual<Ptr<HashSet<Address>>>;

    async fn indexings_blocklist(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
        indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
    ) -> Eventual<Ptr<HashSet<Indexing>>>;

    async fn api_keys(&self) -> Eventual<Ptr<HashMap<String, Arc<APIKey>>>>;

    async fn forward_request_to_l2(
        &self,
        l2_url: &Url,
        original_path: &Uri,
        headers: HeaderMap,
        body: Bytes,
        l2_subgraph_id: Option<SubgraphId>,
    ) -> Response<Body>;

    async fn parse_request(
        &self,
        context: &GatewayRequestContext,
        request: GatewayRequest,
    ) -> Result<Self::Request, Error>;

    async fn block_constraints(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
    ) -> Result<BTreeSet<BlockConstraint>, Error>;

    async fn indexer_request_fee(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
        status: &Self::IndexingStatus,
    ) -> Result<u128, IndexerError>;

    fn deterministic_request(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
        chain: &Chain,
        block_requirements: &BlockRequirements,
        blocks_behind: u64,
    ) -> Result<Self::DeterministicRequest, Error>;

    async fn send_request_to_indexer(
        &self,
        context: &GatewayRequestContext,
        selection: &SelectionInfo,
        request: Self::DeterministicRequest,
    ) -> Result<IndexerResponse, IndexerError>;

    async fn finalize_response(
        &self,
        result: Result<(SelectionInfo, IndexerResponse), Error>,
    ) -> Response<Body>;
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
    pub grt_per_usd: Eventual<NotNan<f64>>,
    pub budgeter: &'static Budgeter,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, G::IndexingStatus>>>,
    pub indexing_performance: IndexingPerformance,
    pub receipt_signer: &'static ReceiptSigner,
    pub kafka_client: &'static KafkaClient,
    pub attestation_domain: &'static Eip712Domain,
}

pub struct GatewayLoggingOptions {
    pub event_handler: EventHandlerFn,
}

pub struct GatewayOptions<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    pub config: GatewayConfig,
    pub gateway_impl: G,
    pub logging: GatewayLoggingOptions,
    pub api: Router<Arc<GatewayState<G>>>,
}

pub struct Gateway<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    // pub config: GatewayConfig,
    // pub gateway_id: String,
    pub state: Arc<GatewayState<G>>,
    // pub http_client: reqwest::Client,
    // pub legacy_signer: &'static SecretKey,
    // pub router: Router,
    // pub auth_handler: AuthContext,
}

impl<G> Gateway<G>
where
    G: GatewayImpl + Send + Sync + 'static,
{
    pub async fn run(options: GatewayOptions<G>) -> Result<(), anyhow::Error> {
        let GatewayOptions {
            config,
            gateway_impl,
            logging,
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

        // Set the Kafka group.id setting to whatever the gateway executable
        // name is (e.g. "subgraph-gateway")
        let mut kafka_config = config.kafka.clone();
        kafka_config.insert(
            "group.id".to_string(),
            gateway_details.executable_name.clone(),
        );

        // Instantiate the Kafka client
        let kafka_client: &'static KafkaClient =
            match KafkaClient::new(&config.kafka.clone().into()) {
                Ok(kafka_client) => Box::leak(Box::new(kafka_client)),
                Err(kafka_client_err) => {
                    tracing::error!(%kafka_client_err);
                    return Err(anyhow!("Failed to create Kafka client"));
                }
            };

        // Initialize logging
        reporting::init(
            kafka_client,
            LoggingOptions {
                executable_name: gateway_details.executable_name,
                json: config.log_json,
                event_handler: logging.event_handler,
            },
        );

        tracing::info!("Gateway starting... ID: {}", gateway_details.id);

        let ip_blocker = IpBlocker::new(indexer_selection.ip_blocker_db.as_deref()).unwrap();

        let chains = Box::leak(Box::new(Chains::new(chains.aliases)));

        let grt_per_usd: Eventual<NotNan<f64>> = match payments.exchange_rate_provider {
            ExchangeRateProvider::Fixed(grt_per_usd) => {
                Eventual::from_value(NotNan::new(grt_per_usd).expect("NAN exchange rate"))
            }
            ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).unwrap(),
        };

        tracing::info!("Waiting for exchange rate...");
        grt_per_usd.value().await.unwrap();

        let special_api_keys = match &config.api_keys {
            Some(ApiKeys::Endpoint { special, .. }) => HashSet::from_iter(special.clone()),
            _ => Default::default(),
        };

        let api_keys = gateway_impl.api_keys().await;

        tracing::info!("Waiting for API keys...");
        api_keys.value().await.unwrap();

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();

        let network_subgraph_client =
            subgraph_client::Client::new(http_client.clone(), network.network_subgraph);
        let subgraphs = gateway_impl
            .publications(
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

        let legacy_indexers = gateway_impl
            .legacy_indexers(indexing_statuses.clone())
            .await;

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
                legacy_indexers,
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
                // TODO: Wait until https://github.com/tokio-rs/axum/pull/2653 is released
                // // disable Nagel's algorithm
                // .tcp_nodelay(true)
                .await
                .expect("Failed to start metrics server");
        });

        let subscriptions = match &payments.subscriptions {
            None => Eventual::from_value(Ptr::default()),
            Some(subscriptions) => subscriptions_subgraph::Client::create(
                subgraph_client::Client::builder(
                    http_client.clone(),
                    subscriptions.subgraph.clone(),
                )
                .with_auth_token(subscriptions.ticket.clone())
                .build(),
            ),
        };

        let auth_handler = AuthContext::create(
            payments.payment_required,
            api_keys,
            special_api_keys,
            subscriptions,
            payments
                .subscriptions
                .iter()
                .flat_map(|s| s.special_signers.clone())
                .collect(),
            payments
                .subscriptions
                .as_ref()
                .map(|s| s.rate_per_query)
                .unwrap_or(0),
            payments
                .subscriptions
                .iter()
                .flat_map(|s| &s.domains)
                .map(|d| (d.chain_id, d.contract))
                .collect(),
        );

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
            indexing_performance: IndexingPerformance::new(indexing_statuses.clone()),
            indexing_statuses,
            receipt_signer,
            kafka_client,
            attestation_domain,
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
                    .layer(RequestTracingLayer::new(network_id))
                    // Set the request ID on the request
                    .layer(SetRequestIdLayer::new(gateway_details.id))
                    // Handle legacy in-path auth, and convert it into a header
                    .layer(middleware::from_fn(legacy_auth_adapter))
                    // Require requests to be authorized
                    .layer(RequireAuthorizationLayer::new(auth_handler))
                    // Check the query rate limit with a 60s reset interval
                    .layer(AddRateLimiterLayer::default()),
            )
            .with_state(state.clone());

        let rate_limiter_slots = 10;
        let rate_limiter: &'static RateLimiter<String> =
            Box::leak(Box::new(RateLimiter::<String>::new(
                rate_limiter_slots * config.ip_rate_limit as usize,
                rate_limiter_slots,
            )));
        eventuals::timer(Duration::from_secs(1))
            .pipe(|_| rate_limiter.rotate_slots())
            .forever();

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
        // TODO: Wait until https://github.com/tokio-rs/axum/pull/2653 is released
        // // disable Nagel's algorithm
        // .tcp_nodelay(true)
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
