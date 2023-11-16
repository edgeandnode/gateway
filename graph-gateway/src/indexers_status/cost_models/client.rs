use graphql_http::http_client::ReqwestExt;
use toolshed::url::Url;

use crate::indexers_status::cost_models::query;

pub async fn send_cost_model_query(
    client: reqwest::Client,
    status_url: Url,
    query: query::CostModelQuery,
) -> anyhow::Result<query::CostModelResponse> {
    let res = client.post(status_url.0).send_graphql(query).await;
    match res {
        Ok(res) => Ok(res?),
        Err(e) => Err(anyhow::anyhow!("Error sending cost model query: {}", e)),
    }
}
