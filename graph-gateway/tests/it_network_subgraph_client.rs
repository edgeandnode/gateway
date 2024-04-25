use std::time::Duration;

use assert_matches::assert_matches;
use graph_gateway::network::subgraph::Client;
use thegraph_core::client::Client as SubgraphClient;
use url::Url;

/// Test helper to get the test url from the environment.
fn test_base_url() -> Url {
    std::env::var("IT_TEST_HOSTED_SERVICE_URL")
        .expect("Missing IT_TEST_HOSTED_SERVICE_URL")
        .parse()
        .expect("Invalid IT_TEST_HOSTED_SERVICE_URL")
}

/// Test helper to build the subgraph url with the given subgraph ID.
fn hosted_service_url_with_subgraph_name(name: impl AsRef<str>) -> Url {
    test_base_url()
        .join(&format!("subgraphs/name/{}", name.as_ref()))
        .expect("Invalid URL")
}

/// The Graph Network Mainnet in the hosted service.
///
/// https://thegraph.com/hosted-service/subgraph/graphprotocol/graph-network-mainnet
const GRAPH_NETWORK_MAINNET_SUBGRAPH_ID: &str = "graphprotocol/graph-network-mainnet";

#[test_with::env(IT_TEST_HOSTED_SERVICE_URL)]
#[tokio::test]
async fn fetch_indexers_and_deserialize() {
    //* Given
    let subgraph_url = hosted_service_url_with_subgraph_name(GRAPH_NETWORK_MAINNET_SUBGRAPH_ID);

    let mut network_subgraph_client = {
        let http_client = reqwest::Client::new();
        let subgraph_client = SubgraphClient::builder(http_client, subgraph_url)
            .with_auth_token(None) // Not required for the hosted service
            .build();
        Client::new(subgraph_client, true)
    };

    //* When
    let indexers = tokio::time::timeout(
        Duration::from_secs(10),
        network_subgraph_client.fetch_indexers(),
    )
    .await
    .expect("Fetching indexers timed out");

    //* Then
    assert_matches!(indexers, Ok(indexers) => {
        assert!(!indexers.is_empty());
    });
}

#[test_with::env(IT_TEST_HOSTED_SERVICE_URL)]
#[tokio::test]
async fn fetch_subgraphs_and_deserialize() {
    //* Given
    let subgraph_url = hosted_service_url_with_subgraph_name(GRAPH_NETWORK_MAINNET_SUBGRAPH_ID);

    let mut network_subgraph_client = {
        let http_client = reqwest::Client::new();
        let subgraph_client = SubgraphClient::builder(http_client, subgraph_url)
            .with_auth_token(None) // Not required for the hosted service
            .build();
        Client::new(subgraph_client, true)
    };

    //* When
    let subgraphs = tokio::time::timeout(
        Duration::from_secs(10),
        network_subgraph_client.fetch_subgraphs(),
    )
    .await
    .expect("Fetching subgraphs timed out");

    //* Then
    assert_matches!(subgraphs, Ok(subgraphs) => {
        assert!(!subgraphs.is_empty());
    });
}

#[test_with::env(IT_TEST_HOSTED_SERVICE_URL)]
#[tokio::test]
async fn fetch_subgraph_no_l2_transfer_support_and_deserialize() {
    //* Given
    let subgraph_url = hosted_service_url_with_subgraph_name(GRAPH_NETWORK_MAINNET_SUBGRAPH_ID);

    let mut network_subgraph_client = {
        let http_client = reqwest::Client::new();
        let subgraph_client = SubgraphClient::builder(http_client, subgraph_url)
            .with_auth_token(None) // Not required for the hosted service
            .build();
        Client::new(subgraph_client, false)
    };

    //* When
    let subgraphs = tokio::time::timeout(
        Duration::from_secs(10),
        network_subgraph_client.fetch_subgraphs(),
    )
    .await
    .expect("Fetching subgraphs timed out");

    //* Then
    assert_matches!(subgraphs, Ok(subgraphs) => {
        assert!(!subgraphs.is_empty());
    });
}
