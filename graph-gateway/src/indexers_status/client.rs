use itertools::Itertools;
use std::collections::HashMap;

use prelude::{anyhow, reqwest, DeploymentId, Url};

use crate::indexers_status::poi::{BlockNumber, ProofOfIndexing, ProofOfIndexingInfo};
use crate::indexers_status::{graphql, query};

pub async fn send_public_pois_query(
    client: reqwest::Client,
    status_url: Url,
    query: query::PublicProofOfIndexingQuery,
) -> anyhow::Result<query::PublicProofOfIndexingResponse> {
    graphql::send_graphql_query(&client, status_url, query).await
}

pub async fn send_queries_and_merge_results(
    client: reqwest::Client,
    url: Url,
    pois: &[ProofOfIndexingInfo],
    batch_size: usize,
) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
    debug_assert!(batch_size > 0, "Batch size must be greater than zero");

    // Build the POI queries batches and create the futures
    let queries = pois
        .iter()
        .map(|info| info.meta())
        .map(
            |(deployment, block_number)| query::PublicProofOfIndexingRequest {
                deployment,
                block_number,
            },
        )
        .chunks(batch_size)
        .into_iter()
        .map(|requests| query::PublicProofOfIndexingQuery {
            requests: requests.collect(),
        })
        .map(|query| send_public_pois_query(client.clone(), url.clone(), query))
        .collect::<Vec<_>>();

    // Send all queries concurrently
    let responses = futures::future::join_all(queries).await;

    let response_map: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> = responses
        .into_iter()
        // TODO: Handle errors (e.g., log them with trace level).
        .filter_map(|response| response.ok())
        .flat_map(|response| response.public_proofs_of_indexing)
        .filter_map(|response| {
            // If the response is missing the POI, skip it.
            let poi = response.proof_of_indexing?;
            Some(((response.deployment, response.block.number), poi.into()))
        })
        .collect::<HashMap<_, _>>();

    response_map
}
