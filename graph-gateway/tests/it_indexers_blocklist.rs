use std::time::Duration;

use assert_matches::assert_matches;
use tokio::time::timeout;

use graph_gateway::indexers_blocklist::check_indexer_pois;
use graph_gateway::indexers_status::client::{
    send_public_pois_query, send_queries_and_merge_results,
};
use graph_gateway::indexers_status::poi::{ProofOfIndexing, ProofOfIndexingInfo};
use graph_gateway::indexers_status::query::{
    BlockNumber, PublicProofOfIndexingQuery, PublicProofOfIndexingRequest, MAX_REQUESTS_PER_QUERY,
};
use prelude::{reqwest, Address, DeploymentId};

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
async fn query_indexer_public_pois() {
    //// Given
    let client = reqwest::Client::new();
    let status_url = "https://testnet-indexer-03-europe-cent.thegraph.com/status"
        .parse()
        .expect("Invalid status url");

    let deployment0 = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");
    let deployment1 = test_deployment_id("QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw");
    let query = PublicProofOfIndexingQuery {
        requests: vec![
            PublicProofOfIndexingRequest {
                deployment: deployment0,
                block_number: 123,
            },
            PublicProofOfIndexingRequest {
                deployment: deployment1,
                block_number: 456,
            },
        ],
    };

    //// When
    let request = send_public_pois_query(client, status_url, query);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    assert_matches!(response, Ok(resp) => {
        assert_eq!(resp.public_proofs_of_indexing.len(), 2);

        assert_eq!(resp.public_proofs_of_indexing[0].deployment, deployment0);
        assert_eq!(resp.public_proofs_of_indexing[0].block.number, 123);

        assert_eq!(resp.public_proofs_of_indexing[1].deployment, deployment1);
        assert_eq!(resp.public_proofs_of_indexing[1].block.number, 456);
    });
}

/// Indexers do not support more than 10 requests at a time. It returns a 500 Internal Server
/// Error with the following message: "query is too expensive".
#[tokio::test]
async fn requests_over_max_requests_per_query_should_fail() {
    //// Given
    const REQUESTS_PER_QUERY_LIMIT: usize = 10;

    let client = reqwest::Client::new();
    let status_url = "https://testnet-indexer-03-europe-cent.thegraph.com/status"
        .parse()
        .expect("Invalid status url");

    let deployment = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");
    let query = PublicProofOfIndexingQuery {
        requests: (1..=REQUESTS_PER_QUERY_LIMIT + 1)
            .map(|i| PublicProofOfIndexingRequest {
                deployment,
                block_number: i as BlockNumber,
            })
            .collect(),
    };

    //// When
    let request = send_public_pois_query(client, status_url, query);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    assert!(response.is_err());
}

#[tokio::test]
async fn send_batched_queries_and_merge_results() {
    //// Given
    let client = reqwest::Client::new();
    let status_url = "https://testnet-indexer-03-europe-cent.thegraph.com/status"
        .parse()
        .expect("Invalid status url");

    let poi = zero_poi();
    let deployment = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");

    let pois_to_query = (1..=MAX_REQUESTS_PER_QUERY + 2)
        .map(|i| ProofOfIndexingInfo {
            proof_of_indexing: poi,
            deployment_id: deployment,
            block_number: i as BlockNumber,
        })
        .collect::<Vec<_>>();

    //// When
    let request =
        send_queries_and_merge_results(client, status_url, &pois_to_query, MAX_REQUESTS_PER_QUERY);
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    assert_eq!(response.len(), MAX_REQUESTS_PER_QUERY + 2);
    assert!(response.contains_key(&(deployment, 1)));
    assert!(response.contains_key(&(deployment, 2)));
    assert!(response.contains_key(&(deployment, 3)));
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
