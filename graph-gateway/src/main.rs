use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::read_to_string;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{self, Context as _};
use axum::async_trait;
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{self, Request},
    middleware,
    response::Response,
    routing, Router, Server,
};
use eventuals::{Eventual, EventualExt as _, Ptr};
use gateway_framework::auth::methods::api_keys::APIKey;
use serde_json::json;
use simple_rate_limiter::RateLimiter;
use thegraph_core::{client as subgraph_client, types::DeploymentId};
use tokio::signal::unix::SignalKind;
use tower_http::cors::{self, CorsLayer};

use gateway_common::types::Indexing;
use gateway_framework::{
    gateway::http::{ApiKeys, Gateway, GatewayImpl, GatewayLoggingOptions, GatewayOptions},
    json,
    network::{discovery::Status, network_subgraph},
    rate_limiter::AddRateLimiterLayer,
    reporting::{EventFilterFn, EventHandlerFn},
    scalar,
    topology::network::{Deployment, Indexer},
};
use graph_gateway::client_query::context::Context;
use graph_gateway::client_query::legacy_auth_adapter::legacy_auth_adapter;
use graph_gateway::client_query::query_id::SetQueryIdLayer;
use graph_gateway::client_query::query_tracing::QueryTracingLayer;
use graph_gateway::client_query::require_auth::RequireAuthorizationLayer;
use graph_gateway::config::Config;
use graph_gateway::indexer_client::IndexerClient;
use graph_gateway::indexers::indexing;
use graph_gateway::indexing_performance::IndexingPerformance;
use graph_gateway::indexings_blocklist::indexings_blocklist;
use graph_gateway::reports::{
    report_client_query, report_indexer_query, CLIENT_QUERY_TARGET, INDEXER_QUERY_TARGET,
};
use graph_gateway::{client_query, indexings_blocklist, subgraph_studio};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

struct SubgraphGatewayOptions {
    config: Config,
}

struct SubgraphGateway {
    config: Config,
    http_client: reqwest::Client,
}

impl SubgraphGateway {
    fn new(options: SubgraphGatewayOptions) -> Self {
        SubgraphGateway {
            config: options.config,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl GatewayImpl for SubgraphGateway {
    async fn publications(
        &self,
        network_subgraph: subgraph_client::Client,
        l2_transfer_support: bool,
    ) -> Eventual<Ptr<Vec<network_subgraph::Subgraph>>> {
        network_subgraph::Client::create(network_subgraph, l2_transfer_support).await
    }

    async fn indexing_statuses(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    ) -> Eventual<Ptr<HashMap<Indexing, Status>>> {
        indexing::statuses(
            deployments.clone(),
            self.http_client.clone(),
            self.config.min_graph_node_version.clone(),
            self.config.min_indexer_version.clone(),
        )
        .await
    }

    async fn legacy_indexers(
        &self,
        indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    ) -> Eventual<Ptr<HashSet<Address>>> {
        indexing_statuses.clone().map(|statuses| async move {
            let legacy_indexers: HashSet<Address> = statuses
                .iter()
                .filter(|(_, status)| status.legacy_scalar)
                .map(|(indexing, _)| indexing.indexer)
                .collect();
            Ptr::new(legacy_indexers)
        })
    }

    async fn indexings_blocklist(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
        indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
    ) -> Eventual<Ptr<HashSet<Indexing>>> {
        // Indexer blocklist
        //
        // Periodically check the defective POIs list against the network indexers
        // and update the indexers blocklist accordingly.
        match &self.config.poi_blocklist {
            Some(blocklist) if !blocklist.pois.is_empty() => {
                let pois = blocklist.pois.clone();
                let update_interval = blocklist
                    .update_interval
                    .map_or(indexings_blocklist::DEFAULT_UPDATE_INTERVAL, |min| {
                        Duration::from_secs(min * 60)
                    });

                indexings_blocklist(
                    self.http_client.clone(),
                    deployments.clone(),
                    indexers.clone(),
                    pois,
                    update_interval,
                )
                .await
            }
            _ => Eventual::from_value(Ptr::default()),
        }
    }

    async fn api_keys(&self) -> Eventual<Ptr<HashMap<String, Arc<APIKey>>>> {
        match self.config.common.api_keys.clone() {
            Some(ApiKeys::Endpoint { url, auth, .. }) => {
                subgraph_studio::api_keys(self.http_client.clone(), url, auth.0)
            }
            Some(ApiKeys::Fixed(api_keys)) => Eventual::from_value(Ptr::new(
                api_keys
                    .into_iter()
                    .map(|k| (k.key.clone(), k.into()))
                    .collect(),
            )),
            None => Eventual::from_value(Ptr::default()),
        }
    }
}

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

    let gateway = Gateway::new(GatewayOptions {
        gateway_impl: SubgraphGateway::new(SubgraphGatewayOptions {
            config: config.clone(),
        }),
        config: config.common,
        logging: GatewayLoggingOptions {
            event_filter: EventFilterFn::new(|metadata| {
                (metadata.target() == CLIENT_QUERY_TARGET)
                    || (metadata.target() == INDEXER_QUERY_TARGET)
            }),
            event_handler: EventHandlerFn::new(|client, metadata, fields| {
                match metadata.target() {
                    CLIENT_QUERY_TARGET => report_client_query(client, fields),
                    INDEXER_QUERY_TARGET => report_indexer_query(client, fields),
                    _ => unreachable!("invalid event target for KafkaLayer"),
                }
            }),
        },
    })
    .await
    .expect("should initialize gateway");

    let Gateway {
        gateway_id,
        l2_gateway,

        chains,
        grt_per_usd,
        http_client,
        attestation_domain,
        bad_indexers,
        legacy_signer,
        receipt_signer,
        network,
        indexing_statuses,
        indexings_blocklist,
        budgeter,
        router,
        kafka_client,
        rate_limiter,
        auth_handler,
    } = gateway;

    tracing::debug!(config = %config_repr);

    let client_query_ctx = Context {
        indexer_client: IndexerClient {
            client: http_client.clone(),
        },
        receipt_signer,
        kafka_client,
        budgeter,
        indexer_selection_retry_limit: config.indexer_selection_retry_limit,
        l2_gateway,
        chains,
        grt_per_usd,
        network,
        indexing_perf: IndexingPerformance::new(indexing_statuses.clone()),
        indexing_statuses,
        attestation_domain,
        bad_indexers,
        indexings_blocklist,
    };

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
            // bottom, and then the response would bubble back up through the layers in reverse
            tower::ServiceBuilder::new()
                .layer(
                    CorsLayer::new()
                        .allow_origin(cors::Any)
                        .allow_headers(cors::Any)
                        .allow_methods([http::Method::OPTIONS, http::Method::POST]),
                )
                // Set up the query tracing span
                .layer(QueryTracingLayer::new(config.graph_env_id.clone()))
                // Set the query ID on the request
                .layer(SetQueryIdLayer::new(gateway_id))
                // Handle legacy in-path auth, and convert it into a header
                .layer(middleware::from_fn(legacy_auth_adapter))
                // Require the query to be authorized
                .layer(RequireAuthorizationLayer::new(auth_handler))
                // Check the query rate limit with a 60s reset interval
                .layer(AddRateLimiterLayer::default()),
        );

    let router = router
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
        )
        .nest("/api", api)
        .layer(middleware::from_fn_with_state(rate_limiter, ip_rate_limit));

    Server::bind(&SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        config.port_api,
    ))
    // disable Nagel's algorithm
    .tcp_nodelay(true)
    .serve(router.into_make_service_with_connect_info::<SocketAddr>())
    .with_graceful_shutdown(await_shutdown_signals())
    .await
    .expect("Failed to start API server");
    tracing::warn!("shutdown");
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

async fn ip_rate_limit<B>(
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

fn graphql_error_response<S: ToString>(message: S) -> json::JsonResponse {
    json::json_response([], json!({"errors": [{"message": message.to_string()}]}))
}
