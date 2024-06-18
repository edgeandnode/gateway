//! A paginated query subgraph client that supports querying the indexer endpoint directly.
//!
//! The client inserts a [`Receipt`](crate::receipts::Receipt) as a header of the request to
//! authenticate the request with the indexer and pays the indexer for the request.

use alloy_primitives::Address;
use serde::Deserialize;
use thegraph_core::{client::Client as PaginatedSubgraphClient, types::DeploymentId};
use thegraph_graphql_http::graphql::Document;
use url::Url;

use super::paginated_client::PaginatedClient;

/// Builds a URL to query an indexer's subgraph.
fn indexer_subgraph_url(base: &Url, deployment: &DeploymentId) -> anyhow::Result<Url> {
    base.join(&format!("subgraphs/id/{}", deployment))
        .map_err(Into::into)
}

/// A client that fetches data from a list of candidate indexers.
///
/// The client will try to fetch data from each candidate in the order they were
/// provided. If a candidate fails to fetch the data, the client will try the
/// next candidate in the list.
///
/// The client will return an error if all candidates fail to fetch the data.
pub struct Client {
    candidates: Vec<(Address, PaginatedSubgraphClient)>,
}

impl Client {
    pub fn new(
        client: reqwest::Client,
        subgraph: DeploymentId,
        candidates: impl IntoIterator<Item = (Address, (Url, String))>,
    ) -> Self {
        let candidates = candidates
            .into_iter()
            .filter_map(|(id, (url, auth))| {
                let url = indexer_subgraph_url(&url, &subgraph).ok()?;
                let client = PaginatedSubgraphClient::builder(client.clone(), url)
                    .with_auth_token(Some(auth))
                    .build();
                Some((id, client))
            })
            .collect();

        Self { candidates }
    }
}

impl PaginatedClient for Client {
    async fn paginated_query<T>(&self, query: Document, page_size: usize) -> Result<Vec<T>, String>
    where
        T: for<'de> Deserialize<'de> + Send,
    {
        for (id, client) in self.candidates.iter() {
            let result = client.paginated_query(query.clone(), page_size).await;

            match result {
                Err(err) => {
                    tracing::warn!(
                        indexer=%id,
                        error=?err,
                        "Failed to fetch network subgraph from indexer",
                    );
                }
                Ok(result) if result.is_empty() => {
                    tracing::warn!(
                        indexer=%id,
                        "Indexer returned an empty response",
                    );
                }
                Ok(result) => return Ok(result),
            }
        }

        Err("no candidate indexers left".to_string())
    }
}
