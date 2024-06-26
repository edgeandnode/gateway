use std::future::Future;

use serde::Deserialize;
use thegraph_graphql_http::graphql::Document;

/// A trait for clients that can fetch paginated data.
pub trait PaginatedClient {
    fn paginated_query<T>(
        &self,
        query: Document,
        page_size: usize,
    ) -> impl Future<Output = Result<Vec<T>, String>> + Send
    where
        T: for<'de> Deserialize<'de> + Send;
}
