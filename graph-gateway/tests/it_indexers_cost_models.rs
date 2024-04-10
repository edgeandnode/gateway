use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::indexers::cost_models;
use tokio::time::timeout;

#[tokio::test]
async fn query_indexer_cost_models() {
    let client = reqwest::Client::new();
    let url = "https://testnet-indexer-03-europe-cent.thegraph.com/cost"
        .parse()
        .unwrap();

    let test_deployments = [];

    let request = cost_models::query(&client, url, &[]);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    assert_matches!(response, Ok(indexing_statuses) => {
        assert!(indexing_statuses.len() == test_deployments.len());
        assert!(test_deployments.iter().all(|deployment| indexing_statuses.iter().any(|status| &status.deployment == deployment)));
    });
}
