use std::time::Duration;

use graph_gateway::{indexers, indexers::cost_models};
use tokio::time::timeout;
use url::Url;

/// Test helper to get the testnet indexer url from the environment.
fn test_indexer_url() -> Url {
    std::env::var("IT_TEST_UPGRADE_INDEXER_URL")
        .expect("Missing IT_TEST_UPGRADE_INDEXER_URL")
        .parse()
        .expect("Invalid IT_TEST_UPGRADE_INDEXER_URL")
}

#[test_with::env(IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn fetch_indexer_cost_models() {
    //* Given
    let client = reqwest::Client::new();
    let url = indexers::cost_url(test_indexer_url());

    let test_deployments = [];

    //* When
    let response = timeout(
        Duration::from_secs(60),
        cost_models::send_request(&client, url, &test_deployments),
    )
    .await
    .expect("timeout");

    //* Then
    assert!(response.is_ok(), "Failed to query cost models");
}
