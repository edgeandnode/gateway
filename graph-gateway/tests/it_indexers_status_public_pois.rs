use std::time::Duration;

use assert_matches::assert_matches;
use tokio::time::timeout;
use toolshed::bytes::DeploymentId;

use graph_gateway::indexers_status::public_poi::client;
use graph_gateway::indexers_status::public_poi::{
    BlockNumber, PublicProofOfIndexingQuery, PublicProofOfIndexingRequest, MAX_REQUESTS_PER_QUERY,
};

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
    let request = client::send_public_poi_query(client, status_url, query);
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

    let client = reqwest::Client::new();
    let status_url = "https://testnet-indexer-03-europe-cent.thegraph.com/status"
        .parse()
        .expect("Invalid status url");

    let deployment = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");
    let query = PublicProofOfIndexingQuery {
        requests: (1..=MAX_REQUESTS_PER_QUERY + 1)
            .map(|i| PublicProofOfIndexingRequest {
                deployment,
                block_number: i as BlockNumber,
            })
            .collect(),
    };

    //// When
    let request = client::send_public_poi_query(client, status_url, query);
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

    let deployment = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");

    let pois_to_query = (1..=MAX_REQUESTS_PER_QUERY + 2)
        .map(|i| (deployment, i as BlockNumber))
        .collect::<Vec<_>>();

    //// When
    let request = client::send_public_poi_queries_and_merge_results(
        client,
        status_url,
        pois_to_query,
        MAX_REQUESTS_PER_QUERY,
    );
    let response = timeout(Duration::from_secs(60), request)
        .await
        .expect("timeout");

    //// Then
    assert_eq!(response.len(), MAX_REQUESTS_PER_QUERY + 2);
    assert!(response.contains_key(&(deployment, 1)));
    assert!(response.contains_key(&(deployment, 2)));
    assert!(response.contains_key(&(deployment, 3)));
}
