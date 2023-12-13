use std::time::Duration;

use assert_matches::assert_matches;
use thegraph::types::DeploymentId;
use tokio::time::timeout;

use graph_gateway::indexers::indexing_statuses;

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

    let test_deployments = [
        test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"),
        test_deployment_id("QmSqxfDGyGenGFPkqw9sqnYar4XgzaioVWNvhw5QQ3RB1U"),
    ];

    //// When
    let request = indexing_statuses::query(client, status_url, &test_deployments);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    assert_matches!(response, Ok(resp) => {
        assert!(resp.indexing_statuses.len() == 2);
        assert!(test_deployments.iter().all(|deployment| resp.indexing_statuses.iter().any(|status| &status.subgraph == deployment)));
    });
}
