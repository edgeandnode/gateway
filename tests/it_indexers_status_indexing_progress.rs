use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::indexers;
use thegraph_core::deployment_id;
use tokio::time::timeout;

/// Test helper to get the testnet indexer url from the environment.
fn test_indexer_url() -> reqwest::Url {
    std::env::var("IT_TEST_UPGRADE_INDEXER_URL")
        .expect("Missing IT_TEST_UPGRADE_INDEXER_URL")
        .parse()
        .expect("Invalid IT_TEST_UPGRADE_INDEXER_URL")
}

#[test_with::env(IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn fetch_indexer_indexing_progress() {
    //* Given
    let client = reqwest::Client::new();
    let status_url = indexers::status_url(test_indexer_url());

    let test_deployments = [
        deployment_id!("QmSWxvd8SaQK6qZKJ7xtfxCCGoRzGnoi2WNzmJYYJW9BXY"),
        deployment_id!("QmUhiH6Z5xo6o3GNzsSvqpGKLmCt6w5WzKQ1yHk6C8AA8S"),
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
    assert_eq!(chain.network, "arbitrum-one");
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
    assert_eq!(chain.network, "arbitrum-one");
    assert_matches!(chain.latest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
    assert_matches!(chain.earliest_block, Some(ref block) => {
        assert!(block.number > 0);
    });
}
