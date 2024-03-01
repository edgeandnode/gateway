use std::{env, fs::read_to_string, path::PathBuf};

use anyhow::{self, Context as _};
use axum::{routing, Router};
use gateway_framework::{
    gateway::http::{Gateway, GatewayLoggingOptions, GatewayOptions},
    reporting::{EventFilterFn, EventHandlerFn},
};
use graph_gateway::{
    self, client_query,
    config::Config,
    gateway::{SubgraphGateway, SubgraphGatewayOptions},
    reports::{
        report_client_query, report_indexer_query, CLIENT_QUERY_TARGET, INDEXER_QUERY_TARGET,
    },
};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

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

    tracing::info!("Starting gateway");

    let config_repr = format!("{config:#?}");
    tracing::debug!(config = %config_repr);

    let gateway_impl = SubgraphGateway::new(SubgraphGatewayOptions {
        config: config.clone(),
    });

    Gateway::run(GatewayOptions {
        gateway_impl,

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

        api: Router::new()
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
            ),
    })
    .await
    .expect("should initialize and run gateway");
}
