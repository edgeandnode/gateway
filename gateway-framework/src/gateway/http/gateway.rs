use std::{
    collections::{HashMap, HashSet},
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{Address, U256};
use alloy_sol_types::Eip712Domain;
use anyhow::{anyhow, Context as _};
use axum::{
    async_trait,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, Request},
    middleware,
    response::Response,
    routing, Router, Server,
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
    types::{attestation, DeploymentId},
};
use tokio::{signal::unix::SignalKind, spawn};
use tower_http::cors::{self, CorsLayer};
use url::Url;

use crate::{
    auth::{context::AuthContext, methods::api_keys::APIKey},
    budgets::{Budgeter, USD},
    chains::Chains,
    gateway::http::config::ApiKeys,
    geoip::GeoIp,
    ipfs, json,
    network::{
        discovery::Status, exchange_rate, indexing_performance::IndexingPerformance,
        network_subgraph::Subgraph,
    },
    reporting::{self, EventFilterFn, EventHandlerFn, KafkaClient, LoggingOptions},
    scalar::{self, ReceiptSigner},
    subscriptions::subgraph as subscriptions_subgraph,
    topology::{
        keep_allocations_up_to_date,
        network::{Deployment, GraphNetwork, Indexer},
    },
};

use super::{
    config::ExchangeRateProvider,
    middleware::{
        legacy_auth_adapter, AddRateLimiterLayer, RequestTracingLayer, RequireAuthorizationLayer,
        SetRequestIdLayer,
    },
    GatewayConfig,
};

#[async_trait]
pub trait GatewayImpl {
    async fn publications(
        &self,
        network_subgraph: subgraph_client::Client,
        l2_transfer_support: bool,
    ) -> Eventual<Ptr<Vec<Subgraph>>>;

    async fn indexing_statuses(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    ) -> Eventual<Ptr<HashMap<Indexing, Status>>>;

    async fn legacy_indexers(
        &self,
        indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    ) -> Eventual<Ptr<HashSet<Address>>>;

    async fn indexings_blocklist(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
        indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
    ) -> Eventual<Ptr<HashSet<Indexing>>>;

    async fn api_keys(&self) -> Eventual<Ptr<HashMap<String, Arc<APIKey>>>>;
}

pub struct GatewayState<G> {
    pub config: GatewayConfig,
    pub gateway_impl: G,

    pub l2_gateway: Option<Url>,
    pub network: GraphNetwork,
    pub chains: &'static Chains,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
    pub bad_indexers: &'static HashSet<Address>,
    pub grt_per_usd: Eventual<NotNan<f64>>,
    pub budgeter: &'static Budgeter,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    pub indexing_performance: IndexingPerformance,
    pub receipt_signer: &'static ReceiptSigner,
    pub kafka_client: &'static KafkaClient,
    pub attestation_domain: &'static Eip712Domain,
}

pub struct GatewayLoggingOptions {
    pub event_filter: EventFilterFn,
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

        // Set the Kafka group.id setting to whatever the gateway executable
        // name is (e.g. "subgraph-gateway")
        let mut kafka_config = config.kafka.clone();
        kafka_config.insert("group.id".to_string(), config.executable_name.clone());

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
                executable_name: config.executable_name,
                json: config.log_json,
                event_filter: logging.event_filter,
                event_handler: logging.event_handler,
            },
        );

        tracing::info!("Gateway starting... ID: {}", config.gateway_id);

        let geoip = config
            .geoip
            .database
            .filter(|_| !config.geoip.blocked_countries.is_empty())
            .map(|db| {
                GeoIp::new(db, config.geoip.blocked_countries)
                    .context("GeoIp")
                    .unwrap()
            });

        let chains = Box::leak(Box::new(Chains::new(config.chain_aliases)));

        let grt_per_usd: Eventual<NotNan<f64>> = match config.exchange_rate_provider {
            ExchangeRateProvider::Fixed(grt_per_usd) => {
                Eventual::from_value(NotNan::new(grt_per_usd).expect("NAN exchange rate"))
            }
            ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await.unwrap(),
        };
        tracing::info!("Waiting for exchange rate...");
        grt_per_usd.value().await.unwrap();

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();

        let network_subgraph_client =
            subgraph_client::Client::new(http_client.clone(), config.network_subgraph);
        let subgraphs = gateway_impl
            .publications(network_subgraph_client, config.l2_gateway.is_some())
            .await;

        let attestation_domain: &'static Eip712Domain =
            Box::leak(Box::new(attestation::eip712_domain(
                U256::from_str_radix(&config.attestations.chain_id, 10)
                    .expect("failed to parse attestation domain chain_id"),
                config.attestations.dispute_manager,
            )));

        let ipfs = ipfs::Client::new(http_client.clone(), config.ipfs, 50);
        let network = GraphNetwork::new(subgraphs, ipfs, geoip).await;

        let bad_indexers: &'static HashSet<Address> =
            Box::leak(Box::new(config.bad_indexers.into_iter().collect()));

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

        keep_allocations_up_to_date(
            receipt_signer,
            network.deployments.clone(),
            indexing_statuses.clone(),
        );

        let query_fees_target =
            USD(NotNan::new(config.query_fees_target).expect("invalid query_fees_target"));
        let budgeter: &'static Budgeter = Box::leak(Box::new(Budgeter::new(query_fees_target)));

        // Host metrics on a separate server with a port that isn't open to
        // public requests
        spawn(async move {
            let router = Router::new().route("/metrics", routing::get(handle_metrics));

            Server::bind(&SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                config.metrics_port,
            ))
            // disable Nagel's algorithm
            .tcp_nodelay(true)
            .serve(router.into_make_service())
            .await
            .expect("Failed to start metrics server");
        });

        let special_api_keys = match &config.api_keys {
            Some(ApiKeys::Endpoint { special, .. }) => HashSet::from_iter(special.clone()),
            _ => Default::default(),
        };

        let api_keys = gateway_impl.api_keys().await;

        let subscriptions = match &config.subscriptions {
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

        let state = Arc::new(GatewayState {
            config: gateway_config,
            gateway_impl,

            l2_gateway: config.l2_gateway,
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
                    .layer(RequestTracingLayer::new(config.graph_env_id.clone()))
                    // Set the request ID on the request
                    .layer(SetRequestIdLayer::new(config.gateway_id))
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

        Server::bind(&SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            config.api_port,
        ))
        // disable Nagel's algorithm
        .tcp_nodelay(true)
        .serve(router.into_make_service_with_connect_info::<SocketAddr>())
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

pub async fn ip_rate_limit<B>(
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
