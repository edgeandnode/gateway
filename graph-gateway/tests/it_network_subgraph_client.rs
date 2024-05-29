use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::network::subgraph_client::Client;
use thegraph_core::client::Client as SubgraphClient;
use url::Url;

/// Test helper to get the test url from the environment.
fn test_base_url() -> Url {
    std::env::var("IT_TEST_ARBITRUM_GATEWAY_URL")
        .expect("Missing IT_TEST_ARBITRUM_GATEWAY_URL")
        .parse()
        .expect("Invalid IT_TEST_ARBITRUM_GATEWAY_URL")
}

/// Test helper to get the test auth token from the environment.
fn test_auth_token() -> String {
    std::env::var("IT_TEST_ARBITRUM_GATEWAY_AUTH").expect("Missing IT_TEST_ARBITRUM_GATEWAY_AUTH")
}

/// Test helper to build the subgraph url with the given deployment ID.
fn url_with_deployment_id(name: impl AsRef<str>) -> Url {
    test_base_url()
        .join(&format!("api/deployments/id/{}", name.as_ref()))
        .expect("Invalid URL")
}

/// The Graph Network Arbitrum in the network.
///
/// https://thegraph.com/explorer/subgraphs/DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp
const GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID: &str = "QmZtNN8NbxjJ1KD5uKBYa7Gj29CT8xypSXnAmXbrLNTQgX";

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_subgraphs_and_deserialize() {
    //* Given
    let subgraph_url = url_with_deployment_id(GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID);
    let auth_token = test_auth_token();

    let network_subgraph_client = {
        let http_client = reqwest::Client::new();
        let subgraph_client = SubgraphClient::builder(http_client, subgraph_url)
            .with_auth_token(Some(auth_token))
            .build();
        Client::new(subgraph_client, true)
    };

    //* When
    let subgraphs = tokio::time::timeout(Duration::from_secs(10), network_subgraph_client.fetch())
        .await
        .expect("Fetching subgraphs timed out");

    //* Then
    assert_matches!(subgraphs, Ok(subgraphs) => {
        assert!(!subgraphs.is_empty());
    });
}

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_subgraph_no_l2_transfer_support_and_deserialize() {
    //* Given
    let subgraph_url = url_with_deployment_id(GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID);
    let auth_token = test_auth_token();

    let network_subgraph_client = {
        let http_client = reqwest::Client::new();
        let subgraph_client = SubgraphClient::builder(http_client, subgraph_url)
            .with_auth_token(Some(auth_token))
            .build();
        Client::new(subgraph_client, false)
    };

    //* When
    let subgraphs = tokio::time::timeout(Duration::from_secs(10), network_subgraph_client.fetch())
        .await
        .expect("Fetching subgraphs timed out");

    //* Then
    assert_matches!(subgraphs, Ok(subgraphs) => {
        assert!(!subgraphs.is_empty());
    });
}
