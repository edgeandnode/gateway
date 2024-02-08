use std::time::Duration;

use assert_matches::assert_matches;
use tokio::time::timeout;

use graph_gateway::indexers::version;

#[tokio::test]
async fn query_indexer_service_version() {
    //// Given
    let client = reqwest::Client::new();
    let version_url = "https://testnet-indexer-03-europe-cent.thegraph.com/version"
        .parse()
        .expect("Invalid version url");

    //// When
    let request = version::query_indexer_service_version(&client, version_url);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    // Assert version is present and greater than 0.1.0
    assert_matches!(response, Ok(version) => {
        assert!(version > semver::Version::new(0, 1, 0));
    });
}
