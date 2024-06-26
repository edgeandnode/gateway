use std::{collections::HashSet, sync::Arc, time::Duration};

use alloy_primitives::Address;
use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::client::Client as SubgraphClient;
use tracing_subscriber::{fmt::TestWriter, EnvFilter};
use url::Url;

use super::{
    fetch_and_preprocess_subgraph_info, fetch_update as internal_fetch_update, InternalState,
    NetworkTopologySnapshot,
};
use crate::network::{
    indexer_addr_blocklist::AddrBlocklist, indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::HostResolver, indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::VersionResolver, subgraph_client::Client, IndexingError,
};

// Test method to initialize the tests tracing subscriber.
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .with_writer(TestWriter::default())
        .try_init();
}

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

/// Test helper to build the service config for the tests.
fn test_service_state(
    addr_blocklist: HashSet<Address>,
    host_blocklist: HashSet<IpNetwork>,
    min_versions: Option<(Version, Version)>,
) -> Arc<InternalState> {
    let indexers_http_client = reqwest::Client::new();
    let indexer_host_resolver = HostResolver::new().expect("Failed to create host resolver");
    let indexer_version_resolver = VersionResolver::new(indexers_http_client.clone());
    let indexer_indexing_progress_resolver =
        IndexingProgressResolver::new(indexers_http_client.clone(), Duration::from_secs(60));
    let indexer_indexing_cost_model_resolver = (
        CostModelResolver::new(indexers_http_client.clone()),
        CostModelCompiler::default(),
    );

    let mut state = InternalState {
        indexer_addr_blocklist: None,
        indexer_host_resolver,
        indexer_host_blocklist: None,
        indexer_version_requirements: Default::default(),
        indexer_version_resolver,
        indexer_indexing_pois_blocklist: None,
        indexer_indexing_progress_resolver,
        indexer_indexing_cost_model_resolver,
    };

    if !addr_blocklist.is_empty() {
        let indexers_addr_blocklist = AddrBlocklist::new(addr_blocklist);
        state.indexer_addr_blocklist = Some(indexers_addr_blocklist);
    }

    if !host_blocklist.is_empty() {
        let indexers_host_blocklist = HostBlocklist::new(host_blocklist);
        state.indexer_host_blocklist = Some(indexers_host_blocklist);
    }

    if let Some((min_indexer_service_version, min_graph_node_version)) = min_versions {
        state
            .indexer_version_requirements
            .min_indexer_service_version = min_indexer_service_version;
        state.indexer_version_requirements.min_graph_node_version = min_graph_node_version;
    }

    Arc::new(state)
}

/// Test helper to fetch, process and construct the network topology snapshot.
async fn fetch_update(service: &InternalState) -> anyhow::Result<NetworkTopologySnapshot> {
    let subgraph_url = url_with_deployment_id(GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID);
    let auth_token = test_auth_token();

    let client = {
        let http_client = reqwest::Client::new();
        let subgraph_client = SubgraphClient::builder(http_client, subgraph_url)
            .with_auth_token(Some(auth_token))
            .build();
        Client::new(subgraph_client, true)
    };

    let network = fetch_and_preprocess_subgraph_info(&client, Duration::from_secs(60)).await?;
    Ok(internal_fetch_update(&network, service).await)
}

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_a_network_topology_update() {
    init_test_tracing();

    //* Given
    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        // Minimum versions, different from the default values to assert the versions are set.
        Some((
            Version::new(0, 0, 1), // Indexer service version
            Version::new(0, 0, 1), // Graph node version
        )),
    );

    //* When
    let network = tokio::time::timeout(Duration::from_secs(60), fetch_update(&service))
        .await
        .expect("Topology fetch did not complete in time (30s)")
        .expect("Failed to fetch network topology");

    //* Then
    // Assert that the network topology is not empty.
    assert!(!network.subgraphs.is_empty(), "Network subgraphs are empty");
    assert!(
        !network.deployments.is_empty(),
        "Network deployments are empty"
    );

    // Assert no internal indexing errors are present
    assert!(
        network
            .subgraphs
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|subgraph| {
                subgraph
                    .indexings
                    .values()
                    .all(|indexing| !matches!(indexing, Err(IndexingError::Internal(_))))
            }),
        "Internal indexing errors found"
    );

    // Given a SUBGRAPH
    //- Assert that it has at least one indexing associated.
    assert!(
        network
            .subgraphs
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|subgraph| !subgraph.indexings.is_empty()),
        "Subgraph has no indexings associated"
    );

    //- Assert that all the associated indexings are indexing the same chain
    assert!(
        network
            .subgraphs
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|subgraph| {
                subgraph
                    .indexings
                    .values()
                    .filter_map(|indexing| indexing.as_ref().ok())
                    .all(|indexing| indexing.chain == subgraph.chain)
            }),
        "Some subgraph indexings are not indexing the same chain"
    );

    //- Assert that all the associated indexings' indexers versions are set.
    assert!(
        network
            .subgraphs
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|subgraph| {
                subgraph
                    .indexings
                    .values()
                    .filter_map(|indexing| indexing.as_ref().ok())
                    .all(|indexing| {
                        indexing.indexer.indexer_service_version >= Version::new(0, 0, 1)
                            && indexing.indexer.graph_node_version >= Version::new(0, 0, 1)
                    })
            }),
        "Subgraph indexings indexer versions are not set"
    );

    //- Assert that some of the associated indexings' have reported a valid cost model.
    assert!(
        network
            .subgraphs
            .values()
            .filter_map(|value| value.as_ref().ok())
            .any(|subgraph| {
                subgraph
                    .indexings
                    .values()
                    .filter_map(|indexing| indexing.as_ref().ok())
                    .any(|indexing| indexing.cost_model.is_some())
            }),
        "No subgraph indexings have a cost model"
    );

    //- Assert that all the associated indexings' indexers versions are set.
    assert!(
        network
            .subgraphs
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|subgraph| {
                subgraph
                    .indexings
                    .values()
                    .filter_map(|indexing| indexing.as_ref().ok())
                    .all(|indexing| {
                        indexing.indexer.indexer_service_version >= Version::new(0, 0, 1)
                            && indexing.indexer.graph_node_version >= Version::new(0, 0, 1)
                    })
            }),
        "Subgraph indexings indexer versions are not set"
    );

    // Given a DEPLOYMENT
    //- Assert that it has at least one indexing associated.
    assert!(
        network
            .deployments
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|deployment| !deployment.indexings.is_empty()),
        "Deployment has no indexings associated"
    );

    //- Assert that all the indexings' are correctly associated with the deployment.
    assert!(
        network
            .deployments
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|deployment| {
                deployment
                    .indexings
                    .iter()
                    .filter_map(|(id, indexing)| {
                        indexing.as_ref().ok().map(|indexing| (id, indexing))
                    })
                    .all(|(indexing_id, indexing)| {
                        indexing_id.deployment == deployment.id
                            && indexing.id.deployment == deployment.id
                    })
            }),
        "Incorrect indexing associated with the deployment"
    );

    //- Assert that all indexings' chain match the deployment chain.
    assert!(
        network
            .deployments
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|deployment| {
                deployment
                    .indexings
                    .values()
                    .filter_map(|indexing| indexing.as_ref().ok())
                    .all(|indexing| indexing.chain == deployment.chain)
            }),
        "Some deployment indexings are not indexing the same chain"
    );

    //- Assert that some of the associated indexings' have reported a valid cost model.
    assert!(
        network
            .deployments
            .values()
            .filter_map(|value| value.as_ref().ok())
            .any(|deployment| {
                deployment
                    .indexings
                    .values()
                    .filter_map(|indexing| indexing.as_ref().ok())
                    .any(|indexing| indexing.cost_model.is_some())
            }),
        "No deployment indexings have a cost model"
    );

    // CROSS-CHECKS
    //- Assert that given a subgraph, all the associated deployments contain the subgraph ID in
    //  their subgraphs list.
    assert!(
        network
            .subgraphs
            .values()
            .filter_map(|value| value.as_ref().ok())
            .all(|subgraph| {
                subgraph
                    .indexings
                    .keys()
                    .map(|id| &id.deployment)
                    .all(|deployment_id| {
                        network
                            .deployments
                            .get(deployment_id)
                            .as_ref()
                            .expect("Deployment not found")
                            .as_ref()
                            .expect("Invalid deployment")
                            .subgraphs
                            .contains(&subgraph.id)
                    })
            }),
        "Subgraph associated deployment not found in the network deployments list"
    );
}
