use std::collections::HashMap;

use itertools::Itertools;
use toolshed::bytes::DeploymentId;
use toolshed::url::Url;

use crate::indexers_status::graphql;
use crate::indexers_status::public_poi::query;
use crate::poi::{BlockNumber, ProofOfIndexing};

// To convert from query OI type into `poi` module POI types.
impl From<query::ProofOfIndexing> for ProofOfIndexing {
    fn from(value: query::ProofOfIndexing) -> Self {
        Self(value.0)
    }
}

pub async fn send_public_poi_query(
    client: reqwest::Client,
    status_url: Url,
    query: query::PublicProofOfIndexingQuery,
) -> anyhow::Result<query::PublicProofOfIndexingResponse> {
    graphql::send_graphql_query(&client, status_url, query).await
}

pub async fn send_public_poi_queries_and_merge_results(
    client: reqwest::Client,
    status_url: Url,
    requests: impl IntoIterator<Item = (DeploymentId, BlockNumber)>,
    batch_size: usize,
) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
    // Build the query batches and create the futures
    let queries = requests
        .into_iter()
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
        .map(|query| send_public_poi_query(client.clone(), status_url.clone(), query))
        .collect::<Vec<_>>();

    // Send all queries concurrently
    let responses = futures::future::join_all(queries).await;

    let response_map: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> = responses
        .into_iter()
        // TODO: Handle errors (e.g., log them with trace level).
        .filter_map(|response| response.ok())
        .flat_map(|response| response.public_proofs_of_indexing)
        .filter_map(|response| {
            // If the response is missing the POI field, skip it.
            let poi = response.proof_of_indexing?.into();
            Some(((response.deployment, response.block.number), poi))
        })
        .collect::<HashMap<_, _>>();

    response_map
}
