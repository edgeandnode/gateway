use semver::Version;
use serde::Deserialize;
use thegraph_graphql_http::http_client::ReqwestExt as _;

pub async fn query_indexer_service_version(
    client: &reqwest::Client,
    version_url: reqwest::Url,
) -> anyhow::Result<Version> {
    let response = client
        .get(version_url)
        .send()
        .await?
        .json::<IndexerVersion>()
        .await?;
    Ok(response.version)
}

pub async fn query_graph_node_version(
    client: &reqwest::Client,
    status_url: reqwest::Url,
) -> anyhow::Result<Version> {
    let query = "{ version { version } }";
    let response: GraphNodeVersion = client.post(status_url).send_graphql(query).await??;
    Ok(response.version.version)
}

#[derive(Debug, Deserialize)]
struct GraphNodeVersion {
    version: IndexerVersion,
}

#[derive(Debug, Deserialize)]
struct IndexerVersion {
    version: Version,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_indexer_version_json() {
        //// Given
        let json = r#"{
            "version": "0.1.0"
        }"#;

        //// When
        let version: IndexerVersion =
            serde_json::from_str(json).expect("Failed to deserialize IndexerVersion");

        //// Then
        assert_eq!(version.version, Version::new(0, 1, 0));
    }
}
