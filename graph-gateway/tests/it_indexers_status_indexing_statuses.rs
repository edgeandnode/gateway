use std::time::Duration;

use assert_matches::assert_matches;
use thegraph::types::DeploymentId;
use tokio::time::timeout;

use graph_gateway::indexers::indexing_statuses::client;

/// Test utility function to create a valid `DeploymentId` with an arbitrary deployment id/ipfs hash.
fn test_deployment_id(deployment: &str) -> DeploymentId {
    deployment.parse().expect("invalid deployment id/ipfs hash")
}

#[tokio::test]
async fn query_indexer_indexing_statuses() {
    //// Given
    let client = reqwest::Client::new();
    let status_url = "https://testnet-indexer-03-europe-cent.thegraph.com/status"
        .parse()
        .expect("Invalid status url");

    let test_deployment = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");

    //// When
    let request = client::send_indexing_statuses_query(client, status_url);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    assert_matches!(response, Ok(resp) => {
        assert!(!resp.indexing_statuses.is_empty());
        assert!(resp.indexing_statuses.iter().any(|status| status.subgraph == test_deployment));
    });
}
