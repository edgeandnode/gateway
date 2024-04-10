use alloy_primitives::bytes::Bytes;
use anyhow::anyhow;
use axum::http::{header, HeaderMap, Response, Uri};
use gateway_framework::{errors::Error, graphql};
use thegraph_core::types::SubgraphId;
use url::Url;

pub async fn forward_request_to_l2(
    client: &reqwest::Client,
    l2_url: &Url,
    original_path: &Uri,
    headers: HeaderMap,
    payload: Bytes,
    l2_subgraph_id: Option<SubgraphId>,
) -> Response<String> {
    // We originally attempted to proxy the user's request, but that resulted in a lot of strange
    // behavior from Cloudflare that was too difficult to debug.
    let l2_path = l2_request_path(original_path, l2_subgraph_id);
    let l2_url = l2_url.join(&l2_path).unwrap();
    tracing::info!(%l2_url, %original_path);
    let headers = headers
        .into_iter()
        .filter_map(|(k, v)| Some((k?, v)))
        .filter(|(k, _)| [header::CONTENT_TYPE, header::AUTHORIZATION, header::ORIGIN].contains(k))
        .collect();
    let response = match client
        .post(l2_url)
        .headers(headers)
        .body(payload)
        .send()
        .await
        .and_then(|response| response.error_for_status())
    {
        Ok(response) => response,
        Err(err) => {
            return graphql::error_response(Error::Internal(anyhow!("L2 gateway error: {err}")))
        }
    };
    let status = response.status();
    if !status.is_success() {
        return graphql::error_response(Error::Internal(anyhow!("L2 gateway error: {status}")));
    }
    let body = match response.text().await {
        Ok(body) => body,
        Err(err) => {
            return graphql::error_response(Error::Internal(anyhow!("L2 gateway error: {err}")))
        }
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}

fn l2_request_path(original_path: &Uri, l2_subgraph_id: Option<SubgraphId>) -> String {
    let mut path = original_path.path().to_string();
    let subgraph_prefix = "subgraphs/id/";
    let subgraph_start = path.find(subgraph_prefix);
    // rewrite path of subgraph queries to the L2 subgraph ID, conserving version constraint
    if let (Some(l2_subgraph_id), Some(replace_start)) = (l2_subgraph_id, subgraph_start) {
        let replace_start = replace_start + subgraph_prefix.len();
        let replace_end = path.find('^').unwrap_or(path.len());
        path.replace_range(replace_start..replace_end, &l2_subgraph_id.to_string());
    }
    path
}

#[cfg(test)]
mod tests {
    use thegraph_core::types::{DeploymentId, SubgraphId};

    use super::l2_request_path;

    #[test]
    fn test_l2_request_path() {
        let deployment: DeploymentId = "QmdveVMs7nAvdBPxNoaMMAYgNcuSroneMctZDnZUgbPPP3"
            .parse()
            .unwrap();
        let l1_subgraph: SubgraphId = "EMRitnR1t3drKrDQSmJMSmHBPB2sGotgZE12DzWNezDn"
            .parse()
            .unwrap();
        let l2_subgraph: SubgraphId = "CVHoVSrdiiYvLcH4wocDCazJ1YuixHZ1SKt34UWmnQcC"
            .parse()
            .unwrap();

        // test deployment route
        let mut original = format!("/api/deployments/id/{deployment}").parse().unwrap();
        let mut expected = format!("/api/deployments/id/{deployment}");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));

        // test subgraph route
        original = format!("/api/subgraphs/id/{l1_subgraph}").parse().unwrap();
        expected = format!("/api/subgraphs/id/{l2_subgraph}");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));

        // test subgraph route with API key prefix
        original = format!("/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{l1_subgraph}")
            .parse()
            .unwrap();
        expected = format!("/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{l2_subgraph}");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));

        // test subgraph route with version constraint
        original = format!("/api/subgraphs/id/{l1_subgraph}^0.0.1")
            .parse()
            .unwrap();
        expected = format!("/api/subgraphs/id/{l2_subgraph}^0.0.1");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));
    }
}
