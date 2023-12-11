use std::time::Duration;

use alloy_primitives::Address;
use thegraph::types::DeploymentId;
use tokio::time::timeout;

use graph_gateway::indexers::public_poi::{ProofOfIndexing, ProofOfIndexingInfo};
use graph_gateway::indexings_blocklist::check_indexer_pois;

/// Test utility function to create a valid `ProofOfIndexingInfo` with an zeros POI.
fn zero_poi() -> ProofOfIndexing {
    ProofOfIndexing::from([0u8; 32])
}

/// Test utility function to create a valid `ProofOfIndexingInfo` with an arbitrary POI.
fn test_poi(poi: &str) -> ProofOfIndexing {
    poi.parse().expect("invalid POI")
}

/// Test utility function to create a valid `DeploymentId` with an arbitrary deployment id/ipfs hash.
fn test_deployment_id(deployment: &str) -> DeploymentId {
    deployment.parse().expect("invalid deployment id/ipfs hash")
}

#[tokio::test]
async fn check_indexer_pois_should_find_matches() {
    //// Given
    let client = reqwest::Client::new();

    let indexer_addr = Address::default();
    let status_url = "https://testnet-indexer-03-europe-cent.thegraph.com/status"
        .parse()
        .expect("Invalid status url");

    let deployment = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");

    let poi_info_block2_match = ProofOfIndexingInfo {
        deployment_id: deployment,
        proof_of_indexing: test_poi(
            "0x565b9acec7d617023fdda42cef9fa73e7940653501a04d542d7b7411f09bafe2",
        ),
        block_number: 2,
    };
    let poi_info_block3_match = ProofOfIndexingInfo {
        proof_of_indexing: test_poi(
            "0x6cc41234cbe6d35753159a5863ba41a0a1a73e636eaa7351490437fd31677781",
        ),
        deployment_id: deployment,
        block_number: 3,
    };

    // One or more POIs can be associated with the same deployment and block number. This example
    // tests that the indexer correctly returns all the matching POIs
    let pois_to_query = vec![
        poi_info_block2_match.clone(),
        ProofOfIndexingInfo {
            proof_of_indexing: zero_poi(),
            deployment_id: deployment,
            block_number: 3,
        },
        poi_info_block3_match.clone(),
        ProofOfIndexingInfo {
            proof_of_indexing: zero_poi(),
            deployment_id: deployment,
            block_number: 1,
        },
    ];

    //// When
    let request = check_indexer_pois(client, indexer_addr, status_url, pois_to_query, 1);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    let (address, pois) = response;
    assert_eq!(address, indexer_addr);
    assert_eq!(pois.len(), 2);
    assert!(pois.contains(&poi_info_block2_match));
    assert!(pois.contains(&poi_info_block3_match));
}
