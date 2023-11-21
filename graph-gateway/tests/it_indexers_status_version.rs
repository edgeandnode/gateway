use std::time::Duration;

use assert_matches::assert_matches;
use tokio::time::timeout;

use graph_gateway::indexers_status::version::client;

#[tokio::test]
async fn query_indexer_public_pois() {
    //// Given
    let client = reqwest::Client::new();
    let version_url = "https://testnet-indexer-03-europe-cent.thegraph.com/version"
        .parse()
        .expect("Invalid version url");

    //// When
    let request = client::send_version_query(client, version_url);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    // Assert version is present and greater than 0.1.0
    assert_matches!(response, Ok(resp) => {
        assert!(resp.version > semver::Version::new(0, 1, 0));
    });
}
