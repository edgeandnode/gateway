use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::{indexers, network};
use thegraph_core::types::DeploymentId;
use tokio::time::timeout;

/// Test helper to get the testnet indexer url from the environment.
fn test_indexer_url() -> reqwest::Url {
    std::env::var("IT_TEST_TESTNET_INDEXER_URL")
        .expect("Missing IT_TEST_TESTNET_INDEXER_URL")
        .parse()
        .expect("Invalid IT_TEST_TESTNET_INDEXER_URL")
}

/// Parse a deployment id from a string.
fn parse_deployment_id(deployment: &str) -> DeploymentId {
    deployment.parse().expect("invalid deployment id")
}

#[test_with::env(IT_TEST_TESTNET_INDEXER_URL)]
#[tokio::test]
async fn fetch_indexer_indexing_progress() {
    //* Given
    let client = reqwest::Client::new();
    let status_url = indexers::status_url(test_indexer_url());

    let test_deployments = [
        parse_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"),
        parse_deployment_id("QmSqxfDGyGenGFPkqw9sqnYar4XgzaioVWNvhw5QQ3RB1U"),
    ];

    //* When
    let response = timeout(
        Duration::from_secs(30),
        indexers::indexing_progress::send_request(&client, status_url, &test_deployments),
    )
    .await
    .expect("timeout");

    //* Then
    let indexing_statuses = response.expect("request failed");

    assert_eq!(indexing_statuses.len(), 2);

    // Status for the first deployment
    let status1 = indexing_statuses
        .iter()
        .find(|status| status.deployment_id == test_deployments[0])
        .expect("missing status for deployment 1");

    assert_eq!(status1.chains.len(), 1);
    let chain = &status1.chains[0];
    assert_eq!(chain.network, "mainnet");
    assert_matches!(chain.latest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
    assert_matches!(chain.earliest_block, Some(ref block) => {
        assert!(block.number > 0);
    });

    // Status for the second deployment
    let status2 = indexing_statuses
        .iter()
        .find(|status| status.deployment_id == test_deployments[1])
        .expect("missing status for deployment 2");

    assert_eq!(status2.chains.len(), 1);
    let chain = &status2.chains[0];
    assert_eq!(chain.network, "mainnet");
    assert_matches!(chain.latest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
    assert_matches!(chain.earliest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
}

// TODO: Move test to the network module
#[test_with::env(IT_TEST_TESTNET_INDEXER_URL)]
#[tokio::test]
async fn resolve_indexer_indexing_progress() {
    //* Given
    let client = reqwest::Client::new();
    let status_url = indexers::status_url(test_indexer_url());

    let test_deployments = [
        parse_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"),
        parse_deployment_id("QmSqxfDGyGenGFPkqw9sqnYar4XgzaioVWNvhw5QQ3RB1U"),
    ];

    //* When
    let indexing_statuses = timeout(
        Duration::from_secs(30),
        network::indexer_indexing_progress_resolver::send_requests(
            &client,
            status_url,
            &test_deployments,
            100,
        ),
    )
    .await
    .expect("timeout");

    //* Then
    assert_eq!(indexing_statuses.len(), 2);

    // Status for the first deployment
    let chain_status1 = indexing_statuses
        .get(&test_deployments[0])
        .expect("missing status for deployment 1")
        .as_ref()
        .expect("fetch failed");

    assert_eq!(chain_status1.len(), 1);
    let chain = &chain_status1[0];
    assert_eq!(chain.network, "mainnet");
    assert_matches!(chain.latest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
    assert_matches!(chain.earliest_block, Some(ref block) => {
        assert!(block.number > 0);
    });

    // Status for the second deployment
    let chain_status2 = indexing_statuses
        .get(&test_deployments[1])
        .expect("missing status for deployment")
        .as_ref()
        .expect("fetch failed");

    assert_eq!(chain_status2.len(), 1);
    let chain = &chain_status2[0];
    assert_eq!(chain.network, "mainnet");
    assert_matches!(chain.latest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
    assert_matches!(chain.earliest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
}
