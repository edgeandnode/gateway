use serde::de::DeserializeOwned;
use thegraph_core::client::Client as CoreClient;
use thegraph_graphql_http::graphql::Document;
use url::Url;

use super::paginated_client::PaginatedClient;

/// A `thegraph-core` client that fetches subgraph data in a paginated way.
pub struct Client {
    inner: CoreClient,
}

impl Client {
    /// Creates a new `Client`.
    pub fn new(client: reqwest::Client, url: Url, auth_token: Option<String>) -> Self {
        Self {
            inner: CoreClient::builder(client, url)
                .with_auth_token(auth_token)
                .build(),
        }
    }
}

impl PaginatedClient for Client {
    async fn paginated_query<T>(&self, query: Document, page_size: usize) -> Result<Vec<T>, String>
    where
        T: for<'de> DeserializeOwned + Send,
    {
        self.inner
            .paginated_query::<T>(query, page_size)
            .await
            .map_err(|e| e.to_string())
    }
}
