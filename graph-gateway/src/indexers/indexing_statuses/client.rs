use graphql_http::http_client::ReqwestExt;
use toolshed::url::Url;

use crate::indexers::indexing_statuses::query;

pub async fn send_indexing_statuses_query(
    client: reqwest::Client,
    status_url: Url,
) -> anyhow::Result<query::IndexingStatusesResponse> {
    let res = client
        .post(status_url.0)
        .send_graphql(query::INDEXING_STATUSES_QUERY_DOCUMENT)
        .await;
    match res {
        Ok(res) => Ok(res?),
        Err(e) => Err(anyhow::anyhow!(
            "Error sending indexing statuses query: {}",
            e
        )),
    }
}
