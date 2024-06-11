use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloy_primitives::Address;
use assert_matches::assert_matches;
use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::types::DeploymentId;
use tokio::sync::Mutex;
use tracing_subscriber::{fmt::TestWriter, EnvFilter};
use url::Url;

use crate::network::{
    indexer_addr_blocklist::AddrBlocklist,
    indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::HostResolver,
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::{
        VersionResolver, DEFAULT_INDEXER_VERSION_CACHE_TTL,
        DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT,
    },
    internal::{
        indexer_processing::{self, IndexerIndexingRawInfo, IndexerRawInfo},
        IndexerError, InternalState, VersionRequirements as IndexerVersionRequirements,
    },
};

// Test method to initialize the tests tracing subscriber.
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .with_writer(TestWriter::default())
        .try_init();
}

/// Test helper to get an [`Address`] from a given string.
fn parse_address(addr: impl AsRef<str>) -> Address {
    addr.as_ref().parse().expect("Invalid address")
}

/// Test helper to get a [`Url`] from a given string.
fn parse_url(url: impl AsRef<str>) -> Url {
    url.as_ref().parse().expect("Invalid URL")
}

/// Test helper to get a [`DeploymentId`] from a given string.
fn parse_deployment_id(deployment_id: impl AsRef<str>) -> DeploymentId {
    deployment_id
        .as_ref()
        .parse()
        .expect("Invalid deployment ID")
}

/// Test helper to get an [`IpNetwork`] from a given string.
fn parse_ip_network(network: impl AsRef<str>) -> IpNetwork {
    network.as_ref().parse().expect("Invalid IP network")
}

/// Test helper to build the service config for the tests.
fn test_service_state(
    addr_blocklist: HashSet<Address>,
    host_blocklist: HashSet<IpNetwork>,
    min_versions: Option<(Version, Version)>,
) -> InternalState {
    let indexers_http_client = reqwest::Client::new();
    let indexer_host_resolver =
        Mutex::new(HostResolver::new().expect("Failed to create host resolver"));
    let indexer_version_resolver = VersionResolver::with_timeout_and_cache_ttl(
        indexers_http_client.clone(),
        DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT, // 1500 ms
        DEFAULT_INDEXER_VERSION_CACHE_TTL,          // 20 minutes
    );
    let indexer_indexing_progress_resolver =
        IndexingProgressResolver::new(indexers_http_client.clone());
    let indexer_indexing_cost_model_resolver = (
        CostModelResolver::new(indexers_http_client.clone()),
        Mutex::new(CostModelCompiler::default()),
    );

    let mut state = InternalState {
        indexer_addr_blocklist: None,
        indexer_host_resolver,
        indexer_host_blocklist: None,
        indexer_version_resolver,
        indexer_version_requirements: IndexerVersionRequirements {
            min_indexer_service_version: Version::new(0, 0, 0),
            min_graph_node_version: Version::new(0, 0, 0),
        },
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

    state
}

#[tokio::test]
async fn block_indexer_by_address() {
    init_test_tracing();

    //* Given
    // The Indexer info for 'https://indexer.upgrade.thegraph.com/' indexer
    let indexer_addr = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
    let indexer_url = parse_url("https://indexer.upgrade.thegraph.com/");
    let indexer = IndexerRawInfo {
        id: indexer_addr,
        url: indexer_url,
        staked_tokens: Default::default(),
        indexings: Default::default(),
    };

    let addr_blocklist = HashSet::from([indexer_addr]);
    let service = test_service_state(
        addr_blocklist,
        Default::default(), // No host blocklist
        Default::default(), // No minimum version requirements
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        indexer_processing::process_info(&service, HashMap::from([(indexer_addr, indexer)])),
    )
    .await
    .expect("topology processing did not complete in time (20s)");

    //* Then
    let indexer_processed_info = res.get(&indexer_addr).expect("indexer not found");

    // Assert that the expected indexer is marked as blocked
    assert!(
        matches!(
            indexer_processed_info,
            Err(IndexerError::BlockedByAddrBlocklist)
        ),
        "indexer not marked as blocked"
    );
}

#[tokio::test]
async fn block_indexer_if_host_resolution_fails() {
    init_test_tracing();

    //* Given
    // A random indexer info with a non-resolvable host
    let indexer_addr = parse_address("0x0000000000000000000000000000000000000000");
    let indexer_url = parse_url("https://non-resolvable-host-29155238.com/");
    let indexer = IndexerRawInfo {
        id: indexer_addr,
        url: indexer_url,
        staked_tokens: Default::default(),
        indexings: Default::default(),
    };

    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        Default::default(), // No minimum version requirements
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        indexer_processing::process_info(&service, HashMap::from([(indexer_addr, indexer)])),
    )
    .await
    .expect("topology processing did not complete in time (20s)");

    //* Then
    let indexer_processed_info = res.get(&indexer_addr).expect("indexer not found");
    assert!(
        matches!(
            indexer_processed_info,
            Err(IndexerError::HostResolutionFailed(_))
        ),
        "indexer not marked as failed"
    );
}

#[tokio::test]
async fn block_indexer_by_host_ip_network() {
    init_test_tracing();

    //* Given
    // The Indexer info for 'https://indexer.upgrade.thegraph.com/' indexer
    let indexer_addr = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
    let indexer_url = parse_url("https://indexer.upgrade.thegraph.com/");
    let indexer = IndexerRawInfo {
        id: indexer_addr,
        url: indexer_url,
        staked_tokens: Default::default(),
        indexings: Default::default(),
    };

    // The IP network of the 'https://indexer.upgrade.thegraph.com/' indexer (IPv4: 104.18.40.31)
    let ip_network = parse_ip_network("104.18.40.0/24");

    let host_blocklist = HashSet::from([ip_network]);
    let service = test_service_state(
        Default::default(), // No address blocklist
        host_blocklist,
        Default::default(), // No minimum version requirements
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        indexer_processing::process_info(&service, HashMap::from([(indexer_addr, indexer)])),
    )
    .await
    .expect("Topology processing did not complete in time (20s)");

    //* Then
    let indexer_info = res.get(&indexer_addr).expect("indexer not found");

    // Assert that the blocked indexer is not present in the indexers processed info
    assert!(
        matches!(indexer_info, Err(IndexerError::BlockedByHostBlocklist)),
        "indexer not marked as blocked"
    );
}

#[tokio::test]
async fn block_indexer_if_indexer_service_version_is_below_min() {
    init_test_tracing();

    //* Given
    // The Indexer info for 'https://indexer.upgrade.thegraph.com/' indexer
    let indexer_addr = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
    let indexer_url = parse_url("https://indexer.upgrade.thegraph.com/");
    let indexer = IndexerRawInfo {
        id: indexer_addr,
        url: indexer_url,
        staked_tokens: Default::default(),
        indexings: Default::default(),
    };

    // Set the minimum indexer service version to block all indexers
    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        Some((
            Version::new(999, 999, 9999), // Indexer service version
            Version::new(0, 0, 0),        // Graph node version
        )),
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        indexer_processing::process_info(&service, HashMap::from([(indexer_addr, indexer)])),
    )
    .await
    .expect("topology processing did not complete in time (20s)");

    //* Then
    let indexer_info = res.get(&indexer_addr).expect("indexer not found");

    // Assert the test indexer is blocked due to the minimum service version
    assert!(
        matches!(
            indexer_info,
            Err(IndexerError::IndexerServiceVersionBelowMin(..))
        ),
        "indexer not marked as blocked due to service version below min"
    );
}

// TODO: Add tests covering the POIs blocklist scenarios:
//  - Block indexer if POIs blocklist resolution fails
//  - Block indexer if all deployments are blocked by the POIs blocklist
//  - Block indexing if the indexer is blocked by the POIs blocklist

#[tokio::test]
async fn process_indexers_info_successfully() {
    init_test_tracing();

    //* Given
    // The Indexer info for 'https://indexer.upgrade.thegraph.com/' indexer
    let indexer_address = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
    let indexer_url = parse_url("https://indexer.upgrade.thegraph.com/");
    let indexing_id = parse_deployment_id("QmZtNN8NbxjJ1KD5uKBYa7Gj29CT8xypSXnAmXbrLNTQgX"); // Network subgraph v1.1.0
    let indexing_largest_allocation = parse_address("0xffe9642282d9ead2db93ddb95cc3772a0ac8707c");

    let indexer = IndexerRawInfo {
        id: indexer_address,
        url: indexer_url.clone(),
        staked_tokens: 100_000_000_000_000_000_000_000,
        indexings: HashMap::from([(
            indexing_id,
            IndexerIndexingRawInfo {
                largest_allocation: indexing_largest_allocation,
                total_allocated_tokens: 0,
            },
        )]),
    };

    // Default service state with no blocklists and no minimum version requirements
    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        Default::default(), // No minimum version requirements
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        indexer_processing::process_info(&service, HashMap::from([(indexer_address, indexer)])),
    )
    .await
    .expect("topology processing did not complete in time (20s)");

    //* Then
    let info = res
        .get(&indexer_address)
        .expect("indexer not found")
        .as_ref()
        .expect("indexer information resolution failed");

    // Assert the test indexer is blocked due to the minimum service version
    assert_eq!(info.id, indexer_address);
    assert_eq!(info.url, indexer_url);
    assert_eq!(info.staked_tokens, 100_000_000_000_000_000_000_000);

    assert_eq!(info.indexings.len(), 1);
    assert_matches!(info.indexings.get(&indexing_id), Some(Ok(indexing_info)) => {
        assert_eq!(indexing_info.largest_allocation, indexing_largest_allocation);
        assert_eq!(indexing_info.total_allocated_tokens, 0);
        assert!(indexing_info.progress.latest_block > 0);
    },
        "indexing info not found for {indexing_id}",
    );
}
