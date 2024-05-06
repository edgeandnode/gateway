use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::{indexers, indexers::cost_models};
use tokio::time::timeout;
use url::Url;

/// Test helper to get the testnet indexer url from the environment.
fn test_indexer_url() -> Url {
    std::env::var("IT_TEST_TESTNET_INDEXER_URL")
        .expect("Missing IT_TEST_TESTNET_INDEXER_URL")
        .parse()
        .expect("Invalid IT_TEST_TESTNET_INDEXER_URL")
}

#[test_with::env(IT_TEST_TESTNET_INDEXER_URL)]
#[tokio::test]
async fn query_indexer_cost_models() {
    //* Given
    let client = reqwest::Client::new();
    let url = indexers::cost_url(test_indexer_url());

    let test_deployments = [];

    //* When
    let request = cost_models::query(&client, url, &test_deployments);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //* Then
    assert_matches!(response, Ok(indexing_statuses) => {
        assert_eq!(indexing_statuses.len(), test_deployments.len());
        assert!(test_deployments.iter().all(|deployment| indexing_statuses.iter().any(|status| &status.deployment == deployment)));
    });
}
