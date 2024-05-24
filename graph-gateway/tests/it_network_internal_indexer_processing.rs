use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::Address;
use anyhow::anyhow;
use graph_gateway::network::{
    indexer_addr_blocklist::AddrBlocklist,
    indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::HostResolver,
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::{VersionResolver, DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT},
    internal::{
        fetch_and_pre_process_indexers_info as internal_fetch_and_pre_process_indexers_info,
        process_indexers_info, types::IndexerRawInfo, IndexerError, InternalState,
    },
    subgraph::Client,
};
use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::client::Client as SubgraphClient;
use tokio::sync::{Mutex, OnceCell};
use tracing_subscriber::{fmt::TestWriter, EnvFilter};
use url::Url;

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

/// Test helper to build the subgraph url with the given subgraph ID.
fn url_with_subgraph_id(name: impl AsRef<str>) -> Url {
    test_base_url()
        .join(&format!("api/subgraphs/id/{}", name.as_ref()))
        .expect("Invalid URL")
}

/// The Graph Network Arbitrum in the network.
///
/// https://thegraph.com/explorer/subgraphs/DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp
const GRAPH_NETWORK_ARBITRUM_SUBGRAPH_ID: &str = "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp";

/// Test helper to get an [`Address`] from a given string.
fn test_address(addr: impl AsRef<str>) -> Address {
    addr.as_ref().parse().expect("Invalid address")
}

/// Test helper to build the service config for the tests.
fn test_service_state(
    addr_blocklist: HashSet<Address>,
    host_blocklist: HashSet<IpNetwork>,
    min_versions: Option<(Version, Version)>,
) -> Arc<InternalState> {
    let indexers_http_client = reqwest::Client::new();
    let indexers_host_resolver =
        Mutex::new(HostResolver::new().expect("Failed to create host resolver"));
    let indexers_version_resolver = VersionResolver::with_timeout(
        indexers_http_client.clone(),
        DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT, // 1500 ms
    );
    let indexers_indexing_status_resolver =
        IndexingProgressResolver::new(indexers_http_client.clone());
    let indexers_cost_model_resolver = (
        CostModelResolver::new(indexers_http_client.clone()),
        Mutex::new(CostModelCompiler::default()),
    );

    let mut state = InternalState {
        indexer_http_client: indexers_http_client.clone(),
        indexer_min_agent_version: Version::new(0, 0, 0),
        indexer_min_graph_node_version: Version::new(0, 0, 0),
        indexer_addr_blocklist: None,
        indexer_host_resolver: indexers_host_resolver,
        indexer_host_blocklist: None,
        indexer_version_resolver: indexers_version_resolver,
        indexer_indexing_pois_blocklist: None,
        indexer_indexing_status_resolver: indexers_indexing_status_resolver,
        indexer_indexing_cost_model_resolver: indexers_cost_model_resolver,
    };

    if !addr_blocklist.is_empty() {
        let indexers_addr_blocklist = AddrBlocklist::new(addr_blocklist);
        state.indexer_addr_blocklist = Some(indexers_addr_blocklist);
    }

    if !host_blocklist.is_empty() {
        let indexers_host_blocklist = HostBlocklist::new(host_blocklist);
        state.indexer_host_blocklist = Some(indexers_host_blocklist);
    }

    if let Some((min_agent_version, min_graph_node_version)) = min_versions {
        state.indexer_min_agent_version = min_agent_version;
        state.indexer_min_graph_node_version = min_graph_node_version;
    }

    Arc::new(state)
}

/// Test suite internal state to store the fetched network topology to avoid fetching it multiple
/// times during the tests.
static FETCHED_NETWORK_INFO: OnceCell<HashMap<Address, IndexerRawInfo>> = OnceCell::const_new();

/// Test helper to fetch the network topology information.
///
/// The network topology information is fetched from the hosted service and pre-processed. The
/// result is cached to avoid fetching it multiple times during the tests.
///
/// This is a wrapper around the `service_internal::fetch_network_topology_info` method.
async fn fetch_and_pre_process_indexers_info() -> HashMap<Address, IndexerRawInfo> {
    FETCHED_NETWORK_INFO
        .get_or_try_init(move || async move {
            let subgraph_url = url_with_subgraph_id(GRAPH_NETWORK_ARBITRUM_SUBGRAPH_ID);
            let auth_token = test_auth_token();

            let mut client = {
                let http_client = reqwest::Client::new();
                let subgraph_client = SubgraphClient::builder(http_client, subgraph_url)
                    .with_auth_token(Some(auth_token))
                    .build();
                Client::new(subgraph_client, true)
            };

            let indexers = internal_fetch_and_pre_process_indexers_info(&mut client)
                .await
                .map_err(|err| {
                    anyhow!("Failed to fetch and pre-process the indexers info: {err}")
                })?;

            Ok::<_, anyhow::Error>(indexers)
        })
        .await
        .cloned()
        .expect("Failed to fetch network topology")
}

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_indexers_info_and_block_an_indexer_by_address() {
    init_test_tracing();

    //* Given
    // The Indexer ID (address) of the 'https://indexer.upgrade.thegraph.com/' indexer
    let address = test_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");

    let addr_blocklist = HashSet::from([address]);
    let service = test_service_state(
        addr_blocklist,
        Default::default(), // No host blocklist
        Default::default(), // No minimum versions
    );

    // Fetch and pre-process the network topology information
    let indexers_info = tokio::time::timeout(
        Duration::from_secs(10),
        fetch_and_pre_process_indexers_info(),
    )
    .await
    .expect("Topology fetch did not complete in time (10s)");

    // Require the pre-processed info to contain the "test indexer"
    assert!(
        indexers_info.keys().any(|addr| *addr == address),
        "Test indexer not found in the indexers info"
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        process_indexers_info(&service, indexers_info),
    )
    .await
    .expect("Topology processing did not complete in time (20s)");

    //* Then
    let indexer_processed_info = res.get(&address).expect("Test indexer not found");

    // Assert that the expected indexer is marked as blocked
    assert!(
        matches!(
            indexer_processed_info,
            Err(IndexerError::BlockedByAddrBlocklist)
        ),
        "Test indexer not marked as blocked"
    );
}

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_indexers_info_and_block_an_indexer_by_host() {
    init_test_tracing();

    //* Given
    // The Indexer ID (address) of the 'https://indexer.upgrade.thegraph.com/' indexer
    let address = test_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");

    // The IP network of the 'https://indexer.upgrade.thegraph.com/' indexer (IPv4: 104.18.40.31)
    let ip_network = "104.18.40.0/24".parse().expect("Invalid IP network");

    let host_blocklist = HashSet::from([ip_network]);
    let service = test_service_state(
        Default::default(), // No address blocklist
        host_blocklist,
        Default::default(), // No minimum versions
    );

    // Fetch and pre-process the network topology information
    let indexers_info = tokio::time::timeout(
        Duration::from_secs(10),
        fetch_and_pre_process_indexers_info(),
    )
    .await
    .expect("Topology fetch did not complete in time (10s)");

    // Require the pre-processed info to contain the "test indexer"
    assert!(
        indexers_info.keys().any(|addr| *addr == address),
        "Test indexer not found in the indexers info"
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        process_indexers_info(&service, indexers_info),
    )
    .await
    .expect("Topology processing did not complete in time (20s)");

    //* Then
    let indexer_processed_info = res.get(&address).expect("Test indexer not found");

    // Assert that the blocked indexer is not present in the indexers processed info
    assert!(
        matches!(
            indexer_processed_info,
            Err(IndexerError::BlockedByHostBlocklist)
        ),
        "Test indexer not marked as blocked"
    );
}

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_indexers_info_and_block_indexer_with_min_agent_version() {
    init_test_tracing();

    //* Given
    // The Indexer ID (address) of the 'https://indexer.upgrade.thegraph.com/' indexer
    let address = test_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");

    // Set the minimum indexer agent version to block all indexers
    let min_versions = Some((
        Version::new(999, 999, 9999), // Indexer agent version
        Version::new(0, 0, 0),        // Graph node version
    ));

    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        min_versions,
    );

    // Fetch and pre-process the network topology information
    let indexers_info = tokio::time::timeout(
        Duration::from_secs(10),
        fetch_and_pre_process_indexers_info(),
    )
    .await
    .expect("Topology fetch did not complete in time (10s)");

    assert!(
        indexers_info.keys().any(|addr| *addr == address),
        "Test indexer not found in the indexers info"
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        process_indexers_info(&service, indexers_info),
    )
    .await
    .expect("Topology processing did not complete in time (20s)");

    //* Then
    let indexer_processed_info = res.get(&address).expect("Test indexer not found");

    // Assert the test indexer is blocked due to the minimum agent version
    assert!(
        matches!(
            indexer_processed_info,
            Err(IndexerError::AgentVersionBelowMin(..))
        ),
        "Test indexer not marked as blocked due to agent version below min"
    );
}
