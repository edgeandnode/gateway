use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloy_primitives::{Address, BlockNumber};
use assert_matches::assert_matches;
use ipnetwork::IpNetwork;
use semver::Version;
use thegraph_core::types::DeploymentId;
use tracing_subscriber::{fmt::TestWriter, EnvFilter};
use url::Url;

use crate::{
    indexers::public_poi::{ProofOfIndexing, ProofOfIndexingInfo},
    network::{
        indexer_addr_blocklist::AddrBlocklist,
        indexer_host_blocklist::HostBlocklist,
        indexer_host_resolver::HostResolver,
        indexer_indexing_cost_model_compiler::CostModelCompiler,
        indexer_indexing_cost_model_resolver::CostModelResolver,
        indexer_indexing_poi_blocklist::PoiBlocklist,
        indexer_indexing_poi_resolver::PoiResolver,
        indexer_indexing_progress_resolver::IndexingProgressResolver,
        indexer_version_resolver::{
            VersionResolver, DEFAULT_INDEXER_VERSION_CACHE_TTL,
            DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT,
        },
        internal::{
            indexer_processing::{self, IndexerIndexingRawInfo, IndexerRawInfo},
            IndexerError, IndexerIndexingError, InternalState,
            VersionRequirements as IndexerVersionRequirements,
        },
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

/// Load the upgrade indexer address from the environment.
fn upgrade_indexer_address() -> Address {
    std::env::var("IT_TEST_UPGRADE_INDEXER_ADDRESS")
        .expect("Missing IT_TEST_UPGRADE_INDEXER_ADDRESS")
        .parse()
        .expect("Invalid IT_TEST_UPGRADE_INDEXER_ADDRESS")
}

/// Load the upgrade indexer URL from the environment.
fn upgrade_indexer_url() -> Url {
    std::env::var("IT_TEST_UPGRADE_INDEXER_URL")
        .expect("Missing IT_TEST_UPGRADE_INDEXER_URL")
        .parse()
        .expect("Invalid IT_TEST_UPGRADE_INDEXER_URL")
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

/// Test helper to get a [`ProofOfIndexing`] from a given string.
fn parse_poi(poi: impl AsRef<str>) -> ProofOfIndexing {
    poi.as_ref().parse().expect("Invalid POI")
}

/// Test helper to build the service config for the tests.
fn test_service_state(
    addr_blocklist: HashSet<Address>,
    host_blocklist: HashSet<IpNetwork>,
    min_versions: Option<(Version, Version)>,
    pois_blocklist: HashSet<((DeploymentId, BlockNumber), ProofOfIndexing)>,
) -> InternalState {
    let indexers_http_client = reqwest::Client::new();
    let indexer_host_resolver = HostResolver::new().expect("Failed to create host resolver");
    let indexer_version_resolver = VersionResolver::with_timeout_and_cache_ttl(
        indexers_http_client.clone(),
        DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT, // 1500 ms
        DEFAULT_INDEXER_VERSION_CACHE_TTL,          // 20 minutes
    );
    let indexer_indexing_progress_resolver =
        IndexingProgressResolver::new(indexers_http_client.clone());
    let indexer_indexing_cost_model_resolver = (
        CostModelResolver::new(indexers_http_client.clone()),
        CostModelCompiler::default(),
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

    if !pois_blocklist.is_empty() {
        let pois_blocklist = pois_blocklist
            .into_iter()
            .map(|((deployment_id, block_number), poi)| ProofOfIndexingInfo {
                deployment_id,
                block_number,
                proof_of_indexing: poi,
            })
            .collect();
        let pois_blocklist = PoiBlocklist::new(pois_blocklist);
        let pois_resolver = PoiResolver::new(indexers_http_client.clone());
        state.indexer_indexing_pois_blocklist = Some((pois_resolver, pois_blocklist));
    }

    if let Some((min_indexer_service_version, min_graph_node_version)) = min_versions {
        state
            .indexer_version_requirements
            .min_indexer_service_version = min_indexer_service_version;
        state.indexer_version_requirements.min_graph_node_version = min_graph_node_version;
    }

    state
}

#[test_with::env(IT_TEST_UPGRADE_INDEXER_ADDRESS, IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn block_indexer_by_address() {
    init_test_tracing();

    //* Given
    // The indexer info
    let indexer_url = upgrade_indexer_url();
    let indexer_addr = upgrade_indexer_address();
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
        Default::default(), // No POIs blocklist
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
        Default::default(), // No POIs blocklist
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

#[test_with::env(IT_TEST_UPGRADE_INDEXER_ADDRESS, IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn block_indexer_by_host_ip_network() {
    init_test_tracing();

    //* Given
    // The indexer info
    let indexer_url = upgrade_indexer_url();
    let indexer_addr = upgrade_indexer_address();
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
        Default::default(), // No POIs blocklist
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

#[test_with::env(IT_TEST_UPGRADE_INDEXER_ADDRESS, IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn block_indexer_if_indexer_service_version_is_below_min() {
    init_test_tracing();

    //* Given
    // The indexer info
    let indexer_url = upgrade_indexer_url();
    let indexer_addr = upgrade_indexer_address();
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
        Default::default(), // No POIs blocklist
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

#[test_with::env(IT_TEST_UPGRADE_INDEXER_ADDRESS, IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn block_indexing_if_blocked_by_pois_blocklist() {
    init_test_tracing();

    //* Given
    // Network subgraph arbitrum v1.1.1
    let deployment_1 = parse_deployment_id("QmSWxvd8SaQK6qZKJ7xtfxCCGoRzGnoi2WNzmJYYJW9BXY");
    // Network subgraph ethereum v1.1.1
    let deployment_2 = parse_deployment_id("QmWaCrvdyepm1Pe6RPkJFT3u8KmaZahAvJEFCt27HRWyK4");

    // The indexer info
    let indexer_url = upgrade_indexer_url();
    let indexer_addr = upgrade_indexer_address();
    let indexer = IndexerRawInfo {
        id: indexer_addr,
        url: indexer_url,
        staked_tokens: Default::default(),
        indexings: HashMap::from([
            (
                deployment_1,
                IndexerIndexingRawInfo {
                    largest_allocation: Default::default(),
                    total_allocated_tokens: 0,
                },
            ),
            (
                deployment_2,
                IndexerIndexingRawInfo {
                    largest_allocation: Default::default(),
                    total_allocated_tokens: 0,
                },
            ),
        ]),
    };

    // Set the POIs blocklist to block the network subgraph arbitrum v1.1.1 indexing only
    // if the returned POI matches the faulty one
    let faulty_poi = (
        (deployment_1, 1337),
        parse_poi("0xf99821910bfe16578caa1c823e99a69091409cd1d9d69f9f83e1a43a770c6fa1"),
    );

    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        Default::default(), // No minimum version requirements
        HashSet::from([faulty_poi]),
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

    // Assert the indexer's indexing is blocked due to the POIs blocklist
    let indexing = indexer_info
        .as_ref()
        .expect("indexer information resolution failed")
        .indexings
        .get(&deployment_1)
        .expect("indexing info not found");

    assert_matches!(
        indexing,
        Err(IndexerIndexingError::BlockedByPoiBlocklist),
        "indexing not marked as blocked due to POIs blocklist"
    );

    // Assert the other deployment is not blocked
    let indexing = indexer_info
        .as_ref()
        .expect("indexer information resolution failed")
        .indexings
        .get(&deployment_2)
        .expect("indexing info not found");

    assert!(indexing.is_ok(), "indexing marked as blocked");
}

#[test_with::env(IT_TEST_UPGRADE_INDEXER_ADDRESS, IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn do_not_block_indexing_if_poi_not_blocked_by_poi_blocklist() {
    init_test_tracing();

    //* Given
    // Network subgraph arbitrum v1.1.1
    let deployment_1 = parse_deployment_id("QmSWxvd8SaQK6qZKJ7xtfxCCGoRzGnoi2WNzmJYYJW9BXY");
    // Network subgraph ethereum v1.1.1
    let deployment_2 = parse_deployment_id("QmWaCrvdyepm1Pe6RPkJFT3u8KmaZahAvJEFCt27HRWyK4");

    // The indexer info
    let indexer_url = upgrade_indexer_url();
    let indexer_addr = upgrade_indexer_address();
    let indexer = IndexerRawInfo {
        id: indexer_addr,
        url: indexer_url,
        staked_tokens: Default::default(),
        indexings: HashMap::from([
            (
                deployment_1,
                IndexerIndexingRawInfo {
                    largest_allocation: Default::default(),
                    total_allocated_tokens: 0,
                },
            ),
            (
                deployment_2,
                IndexerIndexingRawInfo {
                    largest_allocation: Default::default(),
                    total_allocated_tokens: 0,
                },
            ),
        ]),
    };

    // Set the POIs blocklist to block the network subgraph arbitrum v1.1.1 indexing only
    // if the returned POI matches the faulty one.
    //
    let faulty_poi = (
        (deployment_1, 1337),
        // A random POI that does not match the faulty one.
        //
        // The POI for block 1337 of  the network subgraph arbitrum v1.1.1 is:
        // 0xf99821910bfe16578caa1c823e99a69091409cd1d9d69f9f83e1a43a770c6fa1
        parse_poi("0x2b7a6d4ed9fbef02c8aa817dfd9bafb126cadc0f8ebcab736e627ef6d5aab060"),
    );

    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        Default::default(), // No minimum version requirements
        HashSet::from([faulty_poi]),
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

    // Assert the indexer's indexing is blocked due to the POIs blocklist
    let indexing = indexer_info
        .as_ref()
        .expect("indexer information resolution failed")
        .indexings
        .get(&deployment_1)
        .expect("indexing info not found");

    assert!(indexing.is_ok(), "indexing not marked as blocked");
}

#[test_with::env(IT_TEST_UPGRADE_INDEXER_ADDRESS, IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn do_not_block_indexing_if_public_pois_resolution_fails() {
    init_test_tracing();

    //* Given
    // Network subgraph arbitrum v1.1.1
    let deployment_1 = parse_deployment_id("QmSWxvd8SaQK6qZKJ7xtfxCCGoRzGnoi2WNzmJYYJW9BXY");
    // Network subgraph ethereum v1.1.1
    let deployment_2 = parse_deployment_id("QmWaCrvdyepm1Pe6RPkJFT3u8KmaZahAvJEFCt27HRWyK4");

    // The indexer info
    let indexer_url = upgrade_indexer_url();
    let indexer_addr = upgrade_indexer_address();
    let indexer = IndexerRawInfo {
        id: indexer_addr,
        url: indexer_url,
        staked_tokens: Default::default(),
        indexings: HashMap::from([
            (
                deployment_1,
                IndexerIndexingRawInfo {
                    largest_allocation: Default::default(),
                    total_allocated_tokens: 0,
                },
            ),
            (
                deployment_2,
                IndexerIndexingRawInfo {
                    largest_allocation: Default::default(),
                    total_allocated_tokens: 0,
                },
            ),
        ]),
    };

    // Set the POIs blocklist to block the network subgraph arbitrum v1.1.1 indexing only
    // if the returned POI matches the faulty one.
    //
    let faulty_poi = (
        (
            deployment_1,
            u64::MAX, // An absurd block number that will cause the POI resolution to fail
        ),
        parse_poi("0xf99821910bfe16578caa1c823e99a69091409cd1d9d69f9f83e1a43a770c6fa1"),
    );

    let service = test_service_state(
        Default::default(), // No address blocklist
        Default::default(), // No host blocklist
        Default::default(), // No minimum version requirements
        HashSet::from([faulty_poi]),
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

    // Assert the indexer's indexing is blocked due to the POIs blocklist
    let indexing = indexer_info
        .as_ref()
        .expect("indexer information resolution failed")
        .indexings
        .get(&deployment_1)
        .expect("indexing info not found");

    assert!(indexing.is_ok(), "indexing not marked as blocked");
}

#[test_with::env(IT_TEST_UPGRADE_INDEXER_ADDRESS, IT_TEST_UPGRADE_INDEXER_URL)]
#[tokio::test]
async fn process_indexers_info_successfully() {
    init_test_tracing();

    //* Given
    // The indexer info
    let indexer_url = upgrade_indexer_url();
    let indexer_addr = upgrade_indexer_address();
    let indexing_id = parse_deployment_id("QmZtNN8NbxjJ1KD5uKBYa7Gj29CT8xypSXnAmXbrLNTQgX"); // Network subgraph v1.1.0
    let indexing_largest_allocation = parse_address("0xffe9642282d9ead2db93ddb95cc3772a0ac8707c");

    let indexer = IndexerRawInfo {
        id: indexer_addr,
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
        Default::default(), // No POIs blocklist
    );

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        indexer_processing::process_info(&service, HashMap::from([(indexer_addr, indexer)])),
    )
    .await
    .expect("topology processing did not complete in time (20s)");

    //* Then
    let info = res
        .get(&indexer_addr)
        .expect("indexer not found")
        .as_ref()
        .expect("indexer information resolution failed");

    // Assert the test indexer is blocked due to the minimum service version
    assert_eq!(info.id, indexer_addr);
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
