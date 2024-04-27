use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::indexers::indexing_statuses;
use thegraph_core::types::DeploymentId;
use tokio::time::timeout;

/// Test utility function to create a valid `DeploymentId` with an arbitrary deployment id/ipfs hash.
fn test_deployment_id(deployment: &str) -> DeploymentId {
    deployment.parse().expect("invalid deployment id/ipfs hash")
}

#[tokio::test]
async fn query_indexer_indexing_statuses() {
    //* Given
    let client = reqwest::Client::new();
    let status_url = "https://testnet-indexer-03-europe-cent.thegraph.com/status"
        .parse()
        .expect("Invalid status url");

    let test_deployments = [
        test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"),
        test_deployment_id("QmSqxfDGyGenGFPkqw9sqnYar4XgzaioVWNvhw5QQ3RB1U"),
    ];

    //* When
    let request = indexing_statuses::query(&client, status_url, &test_deployments);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //* Then
    assert_matches!(response, Ok(indexing_statuses) => {
        assert!(indexing_statuses.len() == 2);
        assert!(test_deployments.iter().all(|deployment| indexing_statuses.iter().any(|status| &status.subgraph == deployment)));
    });
}
