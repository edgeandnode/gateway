use prelude::{anyhow, reqwest, Url};

use crate::indexers_status::graphql;
use crate::indexers_status::indexing_statuses::query;

pub async fn send_indexing_statuses_query(
    client: reqwest::Client,
    status_url: Url,
) -> anyhow::Result<query::IndexingStatusesResponse> {
    graphql::send_graphql_query(&client, status_url, query::IndexingStatusesQuery).await
}
