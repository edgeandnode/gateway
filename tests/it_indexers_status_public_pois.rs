use std::time::Duration;

use alloy_primitives::BlockNumber;
use assert_matches::assert_matches;
use graph_gateway::indexers;
use thegraph_core::deployment_id;
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
async fn query_indexer_public_pois() {
    //* Given
    let client = reqwest::Client::new();
    let status_url = indexers::status_url(test_indexer_url());

    let deployment0 = deployment_id!("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");
    let deployment1 = deployment_id!("QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw");
    let query = [(deployment0, 123), (deployment1, 456)];

    //* When
    let response = tokio::time::timeout(
        Duration::from_secs(60),
        indexers::public_poi::send_request(&client, status_url, &query),
    )
    .await
    .expect("timeout");

    //* Then
    assert_matches!(response, Ok(resp) => {
        assert_eq!(resp.len(), 2);

        assert_eq!(resp[0].deployment, deployment0);
        assert_eq!(resp[0].block.number, 123);

        assert_eq!(resp[1].deployment, deployment1);
        assert_eq!(resp[1].block.number, 456);
    });
}

/// Indexers do not support more than 10 requests at a time. It returns a 500 Internal Server
/// Error with the following message: "query is too expensive".
#[test_with::env(IT_TEST_TESTNET_INDEXER_URL)]
#[tokio::test]
async fn requests_over_max_requests_per_query_should_fail() {
    //* Given

    let client = reqwest::Client::new();
    let status_url = indexers::status_url(test_indexer_url());

    let deployment = deployment_id!("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");
    let query = (1..=11)
        .map(|i| (deployment, i as BlockNumber))
        .collect::<Vec<_>>();

    //* When
    let response = tokio::time::timeout(
        Duration::from_secs(60),
        indexers::public_poi::send_request(&client, status_url, &query),
    )
    .await
    .expect("timeout");

    //* Then
    assert!(response.is_err());
}
