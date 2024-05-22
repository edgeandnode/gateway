//! Indexer versions resolver.
//!
//! The resolver is responsible for fetching the versions of the indexer agent and graph-node
//! services. If the version takes more than the timeout to resolve, the resolver will return an
//! error.
//!
//! The resolver will perform better if the client provided has a connection pool with the different
//! indexers, as it will be able to reuse already established connections.

use std::time::Duration;

use semver::Version;
use url::Url;

use crate::indexers;

/// The default indexer version resolution timeout.
///
/// This timeout is applied \*independently\* for the agent and graph node versions fetches.
pub const DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT: Duration = Duration::from_millis(1_500);

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
}

impl VersionResolver {
    /// Creates a new [`VersionResolver`] instance with the provided client.
    ///
    /// The resolver will use the default indexer version resolution timeout,
    /// [`DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT`].
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            agent_version_resolution_timeout: DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT,
            graph_node_version_resolution_timeout: DEFAULT_INDEXER_VERSION_RESOLUTION_TIMEOUT,
        }
    }

    /// Creates a new [`VersionResolver`] instance with the provided client and timeout.
    pub fn with_timeout(client: reqwest::Client, timeout: Duration) -> Self {
        Self {
            client,
            agent_version_resolution_timeout: timeout,
            graph_node_version_resolution_timeout: timeout,
        }
    }

    /// Resolves the indexer agent version.
    ///
    /// The version resolution time is upper-bounded by the configured timeout.
    pub async fn resolve_agent_version(&self, url: &Url) -> Result<Version, ResolutionError> {
        let indexer_agent_version_url = indexers::version_url(url);

        tokio::time::timeout(
            self.agent_version_resolution_timeout,
            indexers::version::query_indexer_service_version(
                &self.client,
                indexer_agent_version_url,
            ),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(ResolutionError::FetchError)
    }

    /// Resolves the indexer graph-node version.
    ///
    /// The version resolution time is upper-bounded by the configured timeout.
    pub async fn resolve_graph_node_version(&self, url: &Url) -> Result<Version, ResolutionError> {
        let indexer_graph_node_version_url = indexers::status_url(url);
        tokio::time::timeout(
            self.graph_node_version_resolution_timeout,
            indexers::version::query_graph_node_version(
                &self.client,
                indexer_graph_node_version_url,
            ),
        )
        .await
        .map_err(|_| ResolutionError::Timeout)?
        .map_err(ResolutionError::FetchError)
    }
}
