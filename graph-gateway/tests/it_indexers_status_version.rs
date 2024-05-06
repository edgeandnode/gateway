use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::{indexers, indexers::version};
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
async fn query_indexer_service_version() {
    //* Given
    let client = reqwest::Client::new();
    let version_url = indexers::version_url(test_indexer_url());

    //* When
    let request = version::query_indexer_service_version(&client, version_url);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //* Then
    // Assert version is present and greater than 0.1.0
    assert_matches!(response, Ok(version) => {
        assert!(version > semver::Version::new(0, 1, 0));
    });
}
