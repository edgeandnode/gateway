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
use axum::{async_trait, routing, Router, Server};
use eventuals::{Eventual, EventualExt, Ptr};
use gateway_common::types::Indexing;
use ordered_float::NotNan;
use prometheus::Encoder;
use reqwest::StatusCode;
use secp256k1::SecretKey;
use simple_rate_limiter::RateLimiter;
use thegraph_core::{
    client as subgraph_client,
    types::{attestation, DeploymentId},
};
use tokio::spawn;
use url::Url;

use crate::{
    auth::{context::AuthContext, methods::api_keys::APIKey},
    budgets::{Budgeter, USD},
    chains::Chains,
    gateway::http::config::ApiKeys,
    geoip::GeoIp,
    ipfs,
    network::{discovery::Status, exchange_rate, network_subgraph::Subgraph},
    reporting::{self, EventFilterFn, EventHandlerFn, KafkaClient, LoggingOptions},
    scalar::ReceiptSigner,
    subscriptions::subgraph as subscriptions_subgraph,
    topology::network::{Deployment, GraphNetwork, Indexer},
};

use super::{config::ExchangeRateProvider, GatewayConfig};

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

pub struct GatewayLoggingOptions {
    pub event_filter: EventFilterFn,
    pub event_handler: EventHandlerFn,
}

pub struct GatewayOptions<G>
where
    G: GatewayImpl + Sync + Send + 'static,
{
    pub config: GatewayConfig,
    pub gateway_impl: G,
    pub logging: GatewayLoggingOptions,
}

pub struct Gateway {
    pub gateway_id: String,
    pub l2_gateway: Option<Url>,
    pub chains: &'static Chains,
    pub grt_per_usd: Eventual<NotNan<f64>>,
    pub http_client: reqwest::Client,
    pub attestation_domain: &'static Eip712Domain,
    pub bad_indexers: &'static HashSet<Address>,
    pub receipt_signer: &'static ReceiptSigner,
    pub legacy_signer: &'static SecretKey,
    pub network: GraphNetwork,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
    pub budgeter: &'static Budgeter,
    pub router: Router,
    pub kafka_client: &'static KafkaClient,
    pub rate_limiter: &'static RateLimiter<String>,
    pub auth_handler: AuthContext,
}

impl Gateway {
    pub async fn new<G>(options: GatewayOptions<G>) -> Result<Gateway, anyhow::Error>
    where
        G: GatewayImpl + Sync + Send + 'static,
    {
        let GatewayOptions {
            config,
            gateway_impl,
            ..
        } = options;

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
                event_filter: options.logging.event_filter,
                event_handler: options.logging.event_handler,
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
            subgraph_client::Client::new(http_client.clone(), config.network_subgraph.clone());
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

        eventuals::join((network.deployments.clone(), indexing_statuses.clone()))
            .pipe_async(move |(deployments, indexing_statuses)| async move {
                update_allocations(receipt_signer, &deployments, &indexing_statuses).await;
            })
            .forever();

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

        let rate_limiter_slots = 10;
        let rate_limiter: &'static RateLimiter<String> =
            Box::leak(Box::new(RateLimiter::<String>::new(
                rate_limiter_slots * config.ip_rate_limit as usize,
                rate_limiter_slots,
            )));
        eventuals::timer(Duration::from_secs(1))
            .pipe(|_| rate_limiter.rotate_slots())
            .forever();

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

        let router = Router::new()
            .route("/", routing::get(|| async { "Ready to roll!" }))
            // This path is required by NGINX ingress controller
            .route("/ready", routing::get(|| async { "Ready" }));

        Ok(Gateway {
            gateway_id: config.gateway_id,
            l2_gateway: config.l2_gateway,

            chains,
            grt_per_usd,
            http_client,
            attestation_domain,
            bad_indexers,
            receipt_signer,
            legacy_signer,
            network,
            indexing_statuses,
            indexings_blocklist,
            budgeter,
            router,
            kafka_client,
            rate_limiter,
            auth_handler,
        })
    }
}

async fn update_allocations(
    receipt_signer: &ReceiptSigner,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    indexing_statuses: &HashMap<Indexing, Status>,
) {
    tracing::info!(
        deployments = deployments.len(),
        indexings = deployments
            .values()
            .map(|d| d.indexers.len())
            .sum::<usize>(),
        indexing_statuses = indexing_statuses.len(),
    );

    let mut allocations: HashMap<Indexing, Address> = HashMap::new();
    for (deployment, indexer) in deployments.values().flat_map(|deployment| {
        deployment
            .indexers
            .values()
            .map(|indexer| (deployment.as_ref(), indexer.as_ref()))
    }) {
        let indexing = Indexing {
            indexer: indexer.id,
            deployment: deployment.id,
        };
        allocations.insert(indexing, indexer.largest_allocation);
    }
    receipt_signer.update_allocations(allocations).await;
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
