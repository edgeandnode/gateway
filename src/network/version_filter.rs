use std::collections::HashMap;

use parking_lot::RwLock;
use semver::Version;
use thegraph_graphql_http::http_client::ReqwestExt;
use url::Url;

use crate::errors::UnavailableReason;

pub struct MinimumVersionRequirements {
    pub indexer_service: Version,
    pub graph_node: Version,
}

pub struct VersionFilter {
    http: reqwest::Client,
    min_versions: MinimumVersionRequirements,
    indexer_service_version_cache: RwLock<HashMap<String, Version>>,
    graph_node_version_cache: RwLock<HashMap<String, Version>>,
}

impl VersionFilter {
    pub fn new(http: reqwest::Client, min_versions: MinimumVersionRequirements) -> Self {
        Self {
            http,
            min_versions,
            indexer_service_version_cache: Default::default(),
            graph_node_version_cache: Default::default(),
        }
    }

    pub async fn check(&self, url: &Url) -> Result<(), UnavailableReason> {
        let indexer_service_version = self.fetch_indexer_service_version(url).await?;
        if indexer_service_version < self.min_versions.indexer_service {
            return Err(UnavailableReason::NotSupported(
                "indexer-service version below minimum".to_string(),
            ));
        }

        // TODO: After more graph nodes support reporting their version, we should assume they are
        // on the minimum version if we can't get the version.
        match self.fetch_graph_node_version(url).await {
            Ok(version) if version < self.min_versions.graph_node => {
                return Err(UnavailableReason::NotSupported(
                    "graph-node version below minimum".to_string(),
                ));
            }
            _ => (),
        };

        Ok(())
    }

    async fn fetch_indexer_service_version(&self, url: &Url) -> Result<Version, UnavailableReason> {
        if let Some(version) = self.indexer_service_version_cache.read().get(url.as_str()) {
            return Ok(version.clone());
        }

        let url = url
            .join("version")
            .map_err(|_| UnavailableReason::invalid_url())?;
        #[derive(Debug, serde::Deserialize)]
        struct Response {
            version: Version,
        }
        let version = self
            .http
            .get(url.clone())
            .send()
            .await
            .map_err(|_| UnavailableReason::NoStatus("indexer not available".to_string()))?
            .json::<Response>()
            .await
            .map_err(|_| UnavailableReason::NoStatus("indexer not available".to_string()))?
            .version;

        self.indexer_service_version_cache
            .write()
            .insert(url.to_string(), version.clone());

        Ok(version)
    }

    async fn fetch_graph_node_version(&self, url: &Url) -> Result<Version, UnavailableReason> {
        if let Some(version) = self.graph_node_version_cache.read().get(url.as_str()) {
            return Ok(version.clone());
        }

        let url = url
            .join("status")
            .map_err(|_| UnavailableReason::invalid_url())?;
        #[derive(Debug, serde::Deserialize)]
        struct Response {
            version: InnerVersion,
        }
        #[derive(Debug, serde::Deserialize)]
        struct InnerVersion {
            version: Version,
        }
        let version = self
            .http
            .post(url.clone())
            .send_graphql::<Response>("{ version { version } }")
            .await
            .map_err(|err| {
                UnavailableReason::NoStatus(format!("failed to query graph-node version: {err}"))
            })?
            .map_err(|err| {
                UnavailableReason::NoStatus(format!("failed to query graph-node version: {err}"))
            })?
            .version
            .version;

        self.graph_node_version_cache
            .write()
            .insert(url.to_string(), version.clone());

        Ok(version)
    }
}
