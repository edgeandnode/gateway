pub use semver::Version;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct IndexerVersion {
    pub version: Version,
}

pub mod client {
    use toolshed::url::Url;

    use super::IndexerVersion;

    /// Sends a version query to the indexer and returns the version.
    pub async fn send_version_query(
        client: reqwest::Client,
        version_url: Url,
    ) -> anyhow::Result<IndexerVersion> {
        let version = client
            .get(version_url.0)
            .send()
            .await
            .map_err(|err| anyhow::anyhow!("IndexerVersionError({err})"))?
            .json::<IndexerVersion>()
            .await
            .map_err(|err| anyhow::anyhow!("IndexerVersionError({err})"))?;

        Ok(version)
    }
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
