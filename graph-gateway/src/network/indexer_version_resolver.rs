//! Indexer versions resolver.
//!
//! The resolver is responsible for fetching the versions of the indexer agent and graph-node
//! services. If the version takes more than the timeout to resolve, the resolver will return an
//! error.
//!
//! The resolver will perform better if the client provided has a connection pool with the different
//! indexers, as it will be able to reuse already established connections.

use std::{sync::Arc, time::Duration};

use gateway_common::ttl_hash_map::TtlHashMap;
use semver::Version;
use tokio::sync::RwLock;
use url::Url;

use crate::indexers;

/// The default indexer version resolution timeout: 1.5 seconds.
pub const DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT: Duration = Duration::from_millis(1_500);

/// The default TTL (time-to-live) for cache entries: 20 minutes.
pub const DEFAULT_INDEXER_VERSION_CACHE_TTL: Duration = Duration::from_secs(20 * 60);

/// The error that can occur while resolving the indexer versions.
#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {
    /// An error occurred while querying the indexer version.
    #[error("fetch error: {0}")]
    FetchError(anyhow::Error),

    /// The resolution timed out.
    #[error("timeout")]
    Timeout,
}

/// The indexer versions resolver.
///
/// The resolver is responsible for fetching the versions of the indexer agent and graph-node
/// services. If the version takes more than the timeout to resolve, the resolver will return an
/// error.
// TODO: Cache the result with TTL in case the resolution fails.
#[derive(Clone)]
pub struct VersionResolver {
    /// The indexer client.
    ///
    /// Providing a client with a connection pool with the different indexers will reduce
    /// significantly the time to resolve the versions as the resolver will be able to reuse
    /// already established connections.
    client: reqwest::Client,

    /// The indexer agent version resolution timeout.
    agent_version_resolution_timeout: Duration,
    /// The indexer graph-node version resolution timeout.
    graph_node_version_resolution_timeout: Duration,

    /// Cache for the resolved indexer agent versions.
    agent_version_cache: Arc<RwLock<TtlHashMap<String, Version>>>,
    /// Cache for the resolved indexer graph-node versions.
    graph_node_version_cache: Arc<RwLock<TtlHashMap<String, Version>>>,
}

impl VersionResolver {
    /// Creates a new [`VersionResolver`] instance with the provided client.
    ///
    /// The resolver will use the default indexer version resolution timeout,
    /// [`DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT`] (1.5 seconds), and cache TTL,
    /// [`DEFAULT_INDEXER_VERSION_CACHE_TTL`] (20 minutes).
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            agent_version_resolution_timeout: DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT,
            graph_node_version_resolution_timeout: DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT,
            agent_version_cache: Arc::new(RwLock::new(TtlHashMap::with_ttl(
                DEFAULT_INDEXER_VERSION_CACHE_TTL,
            ))),
            graph_node_version_cache: Arc::new(RwLock::new(TtlHashMap::with_ttl(
                DEFAULT_INDEXER_VERSION_CACHE_TTL,
            ))),
        }
    }

    /// Creates a new [`VersionResolver`] instance with the provided client, timeout and cache TTL.
    pub fn with_timeout_and_cache_ttl(
        client: reqwest::Client,
        timeout: Duration,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            client,
            agent_version_resolution_timeout: timeout,
            graph_node_version_resolution_timeout: timeout,
            agent_version_cache: Arc::new(RwLock::new(TtlHashMap::with_ttl(cache_ttl))),
            graph_node_version_cache: Arc::new(RwLock::new(TtlHashMap::with_ttl(cache_ttl))),
        }
    }

    /// Fetches the indexer agent version from the given URL.
    async fn fetch_agent_version(&self, url: &Url) -> Result<Version, ResolutionError> {
        tokio::time::timeout(
            self.agent_version_resolution_timeout,
            indexers::version::query_indexer_service_version(&self.client, url.clone()),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(ResolutionError::FetchError)
    }

    /// Fetches the indexer graph-node version from the given URL.
    async fn fetch_graph_node_version(&self, url: &Url) -> Result<Version, ResolutionError> {
        tokio::time::timeout(
            self.graph_node_version_resolution_timeout,
            indexers::version::query_graph_node_version(&self.client, url.clone()),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(ResolutionError::FetchError)
    }

    /// Resolves the indexer agent version.
    ///
    /// The version resolution time is upper-bounded by the configured timeout.
    pub async fn resolve_agent_version(&self, url: &Url) -> Result<Version, ResolutionError> {
        let indexer_agent_version_url = indexers::version_url(url);
        let indexer_agent_version_url_string = indexer_agent_version_url.to_string();

        let version = match self.fetch_agent_version(&indexer_agent_version_url).await {
            Ok(version) => version,
            Err(err) => {
                tracing::debug!(
                    version_url = indexer_agent_version_url_string,
                    error = ?err,
                    "indexer agent version resolution failed"
                );

                // Try to get the version from the cache, otherwise return the fetch error
                let cache = self.agent_version_cache.read().await;
                return if let Some(version) = cache.get(&indexer_agent_version_url_string) {
                    Ok(version.clone())
                } else {
                    Err(err)
                };
            }
        };

        // Update the cache with the resolved version
        {
            let mut cache = self.agent_version_cache.write().await;
            cache.insert(indexer_agent_version_url_string, version.clone());
        }

        Ok(version)
    }

    /// Resolves the indexer graph-node version.
    ///
    /// The version resolution time is upper-bounded by the configured timeout.
    pub async fn resolve_graph_node_version(&self, url: &Url) -> Result<Version, ResolutionError> {
        let indexer_graph_node_version_url = indexers::version_url(url);
        let indexer_graph_node_version_url_string = indexer_graph_node_version_url.to_string();

        let version = match self
            .fetch_graph_node_version(&indexer_graph_node_version_url)
            .await
        {
            Ok(version) => version,
            Err(err) => {
                tracing::debug!(
                    version_url = indexer_graph_node_version_url_string,
                    error = ?err,
                    "indexer graph-node version resolution failed"
                );

                // Try to get the version from the cache, otherwise return the fetch error
                let cache = self.graph_node_version_cache.read().await;
                return if let Some(version) = cache.get(&indexer_graph_node_version_url_string) {
                    Ok(version.clone())
                } else {
                    Err(err)
                };
            }
        };

        // Update the cache with the resolved version
        {
            let mut cache = self.graph_node_version_cache.write().await;
            cache.insert(indexer_graph_node_version_url_string, version.clone());
        }

        Ok(version)
    }
}
