use semver::Version;
use serde::Deserialize;
use thegraph_graphql_http::http_client::ReqwestExt as _;

use super::urls::{StatusUrl, VersionUrl};

/// Fetches the version of the indexer service at the given URL.
///
/// This function sends a GET request to the indexer service's `/version` endpoint.
pub async fn fetch_indexer_service_version(
    client: &reqwest::Client,
    url: VersionUrl,
) -> anyhow::Result<Version> {
    let response = client
        .get(url.into_inner())
        .send()
        .await?
        .json::<IndexerVersion>()
        .await?;
    Ok(response.version)
}

/// Fetches the version of the graph-node service at the given URL.
///
/// Sends a POST request to the graph-node service's `/status` endpoint with the query
/// `"{ version { version } }"`.
pub async fn fetch_graph_node_version(
    client: &reqwest::Client,
    url: StatusUrl,
) -> anyhow::Result<Version> {
    let query = "{ version { version } }";
    let response: GraphNodeVersion = client.post(url.into_inner()).send_graphql(query).await??;
    Ok(response.version.version)
}

#[derive(Debug, Deserialize)]
struct IndexerVersion {
    version: Version,
}

#[derive(Debug, Deserialize)]
struct GraphNodeVersion {
    version: IndexerVersion,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_indexer_version_json() {
        //* Given
        let json = serde_json::json!({
            "version": "0.1.0"
        });

        //* When
        let res = serde_json::from_value(json);

        //* Then
        let version: IndexerVersion = res.expect("Failed to deserialize IndexerVersion");
        assert_eq!(version.version, Version::new(0, 1, 0));
    }
}
