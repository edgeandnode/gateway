//! Indexer versions resolver.
//!
//! The resolver is responsible for fetching the versions of the indexer service and graph-node
//! services. If the version takes more than the timeout to resolve, the resolver will return an
//! error.
//!
//! The resolver will perform better if the client provided has a connection pool with the different
//! indexers, as it will be able to reuse already established connections.

use std::{collections::HashMap, sync::Arc, time::Duration};

use parking_lot::RwLock;
use semver::Version;
use url::Url;

use crate::indexers;

/// The default indexer version resolution timeout: 5 seconds.
pub const DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT: Duration = Duration::from_millis(5_000);

/// The error that can occur while resolving the indexer versions.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ResolutionError {
    /// An error occurred while querying the indexer version.
    #[error("fetch error: {0}")]
    FetchError(String),

    /// The resolution timed out.
    #[error("timeout")]
    Timeout,
}

/// The indexer versions resolver.
///
/// The resolver is responsible for fetching the versions of the indexer service and graph-node
/// services. If the version takes more than the timeout to resolve, the resolver will return an
/// error.
#[derive(Clone)]
pub struct VersionResolver {
    /// The indexer client.
    ///
    /// Providing a client with a connection pool with the different indexers will reduce
    /// significantly the time to resolve the versions as the resolver will be able to reuse
    /// already established connections.
    client: reqwest::Client,

    /// The indexer service version resolution timeout.
    indexer_service_version_resolution_timeout: Duration,
    /// The indexer graph-node version resolution timeout.
    graph_node_version_resolution_timeout: Duration,

    /// Cache for the resolved indexer service versions.
    indexer_service_version_cache: Arc<RwLock<HashMap<String, Version>>>,
    /// Cache for the resolved indexer graph-node versions.
    graph_node_version_cache: Arc<RwLock<HashMap<String, Version>>>,
}

impl VersionResolver {
    pub fn new(client: reqwest::Client, timeout: Duration) -> Self {
        Self {
            client,
            indexer_service_version_resolution_timeout: timeout,
            graph_node_version_resolution_timeout: timeout,
            indexer_service_version_cache: Default::default(),
            graph_node_version_cache: Default::default(),
        }
    }

    /// Fetches the indexer service version from the given URL.
    async fn fetch_indexer_service_version(
        &self,
        url: &indexers::VersionUrl,
    ) -> Result<Version, ResolutionError> {
        tokio::time::timeout(
            self.indexer_service_version_resolution_timeout,
            indexers::version::fetch_indexer_service_version(&self.client, url.clone()),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(|err| ResolutionError::FetchError(err.to_string()))
    }

    /// Fetches the indexer graph-node version from the given URL.
    async fn fetch_graph_node_version(
        &self,
        url: &indexers::StatusUrl,
    ) -> Result<Version, ResolutionError> {
        tokio::time::timeout(
            self.graph_node_version_resolution_timeout,
            indexers::version::fetch_graph_node_version(&self.client, url.clone()),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(|err| ResolutionError::FetchError(err.to_string()))
    }

    /// Resolves the indexer service version.
    ///
    /// The version resolution time is upper-bounded by the configured timeout.
    pub async fn resolve_indexer_service_version(
        &self,
        url: &Url,
    ) -> Result<Version, ResolutionError> {
        let url_string = url.to_string();
        let version_url = indexers::version_url(url);

        let version = match self.fetch_indexer_service_version(&version_url).await {
            Ok(version) => version,
            Err(err) => {
                tracing::debug!(
                    version_url = url_string,
                    error = ?err,
                    "indexer service version resolution failed"
                );

                // Try to get the version from the cache, otherwise return the fetch error
                let cache = self.indexer_service_version_cache.read();
                return if let Some(version) = cache.get(&url_string) {
                    Ok(version.clone())
                } else {
                    Err(err)
                };
            }
        };

        // Update the cache with the resolved version
        {
            let mut cache = self.indexer_service_version_cache.write();
            cache.insert(url_string, version.clone());
        }

        Ok(version)
    }

    /// Resolves the indexer graph-node version.
    ///
    /// The version resolution time is upper-bounded by the configured timeout.
    pub async fn resolve_graph_node_version(&self, url: &Url) -> Result<Version, ResolutionError> {
        let url_string = url.to_string();
        let status_url = indexers::status_url(url);

        let version = match self.fetch_graph_node_version(&status_url).await {
            Ok(version) => version,
            Err(err) => {
                tracing::debug!(
                    version_url = url_string,
                    error = ?err,
                    "indexer graph-node version resolution failed"
                );

                // Try to get the version from the cache, otherwise return the fetch error
                let cache = self.graph_node_version_cache.read();
                return if let Some(version) = cache.get(&url_string) {
                    Ok(version.clone())
                } else {
                    Err(err)
                };
            }
        };

        // Update the cache with the resolved version
        {
            let mut cache = self.graph_node_version_cache.write();
            cache.insert(url_string, version.clone());
        }

        Ok(version)
    }
}
