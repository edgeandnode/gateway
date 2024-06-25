use std::{env, fs::read_to_string, path::PathBuf};

use anyhow::Context as _;
use axum::{routing, Router};
use gateway_framework::gateway::http::{Gateway, GatewayOptions};
use graph_gateway::{
    config::Config,
    gateway::{SubgraphGateway, SubgraphGatewayOptions},
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

    let gateway_impl = SubgraphGateway::new(SubgraphGatewayOptions {
        config: config.clone(),
    });

    Gateway::run(GatewayOptions {
        gateway_impl,
        config: config.common,
        api: Router::new()
            .route(
                "/deployments/id/:deployment_id",
                routing::post(Gateway::handle_request),
            )
            .route(
                "/:api_key/deployments/id/:deployment_id",
                routing::post(Gateway::handle_request),
            )
            .route(
                "/subgraphs/id/:resource_id",
                routing::post(Gateway::handle_request),
            )
            .route(
                "/:api_key/subgraphs/id/:resource_id",
                routing::post(Gateway::handle_request),
            ),
    })
    .await
    .expect("should initialize and run gateway");
}
