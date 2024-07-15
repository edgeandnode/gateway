//! A paginated query subgraph client that supports querying the indexer endpoint directly.
//!
//! The client inserts an `Authorization` header with the indexer's free query auth token to
//! authenticate the request with the indexer.

use custom_debug::CustomDebug;
use gateway_framework::config::Hidden;
use inner_client::Client as PaginatedIndexerSubgraphClient;
use serde::Deserialize;
use serde_with::serde_as;
use thegraph_graphql_http::graphql::Document;
use url::Url;

use super::paginated_client::PaginatedClient;

mod inner_client;
mod queries;

/// A client that fetches data from a list of trusted indexers.
///
/// The client will try to fetch data from each indexer in the order they are
/// provided. The client will return an error if requests to all idnexers fail.
pub struct Client {
    inner: PaginatedIndexerSubgraphClient,
    indexers: Vec<TrustedIndexer>,
}

#[serde_as]
#[derive(CustomDebug, Deserialize)]
pub struct TrustedIndexer {
    /// network subgraph endpoint
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    /// free query auth token
    pub auth: Hidden<String>,
}

impl Client {
    pub fn new(client: reqwest::Client, indexers: Vec<TrustedIndexer>) -> Self {
        let inner = PaginatedIndexerSubgraphClient::new(client);
        Self { inner, indexers }
    }
}

impl PaginatedClient for Client {
    async fn paginated_query<T>(&self, query: Document, page_size: usize) -> Result<Vec<T>, String>
    where
        T: for<'de> Deserialize<'de> + Send,
    {
        for indexer in self.indexers.iter() {
            let result = self
                .inner
                .paginated_query(indexer.url.clone(), &indexer.auth, query.clone(), page_size)
                .await;

            match result {
                Err(err) => {
                    tracing::warn!(
                        indexer_url=%indexer.url,
                        error=?err,
                        "Failed to fetch network subgraph from indexer",
                    );
                }
                Ok(result) if result.is_empty() => {
                    tracing::warn!(
                        indexer_url=%indexer.url,
                        "Indexer returned an empty response",
                    );
                }
                Ok(result) => return Ok(result),
            }
        }

        Err("no candidate indexers left".to_string())
    }
}
