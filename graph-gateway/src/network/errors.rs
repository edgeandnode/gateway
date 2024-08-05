use semver::Version;

use crate::network::{
    indexer_host_resolver::ResolutionError as HostResolutionError,
    indexer_version_resolver::ResolutionError as VersionResolutionError,
};

/// Subgraph validation error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum SubgraphError {
    /// No allocations were found for the subgraph.
    #[error("no allocations")]
    NoAllocations,

    /// All subgraph versions were marked as invalid.
    #[error("no valid versions")]
    NoValidVersions,
}

/// Deployment validation error
#[derive(Clone, Debug, thiserror::Error)]
pub enum DeploymentError {
    /// No allocations were found for the subgraph.
    #[error("no allocations")]
    NoAllocations,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ResolutionError {
    /// The indexing is unavailable.
    #[error(transparent)]
    Unavailable(UnavailableReason),

    /// Errors that should only occur in exceptional conditions.
    #[error("internal error: {0}")]
    Internal(&'static str),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum UnavailableReason {
    /// Blocked by one of the blocklists.
    #[error("blocked")]
    Blocked,
    /// The indexing was blocked since it reported a bad POI blocked by the gateway.
    #[error("blocked (bad POI)")]
    BlockedBadPOI,

    /// The indexing is unavailable due to an [`IndexerInfoResolutionError`].
    #[error("{0}")]
    IndexerResolutionError(&'static str),

    /// Indexer service version is below the minimum required.
    #[error("indexer service version below the minimum required")]
    IndexerServiceVersionBelowMin,
    /// Graph node version is below the minimum required.
    #[error("graph node version below the minimum required")]
    GraphNodeVersionBelowMin,

    /// The indexing progress information was not found.
    ///
    /// The resolution failed, and it was not available in the cache.
    #[error("indexing progress not found")]
    IndexingProgressNotFound,
}

impl From<IndexingError> for ResolutionError {
    fn from(error: IndexingError) -> Self {
        match error {
            IndexingError::Indexer(err) => {
                let reason = match err {
                    IndexerInfoResolutionError::BlockedByAddrBlocklist => {
                        UnavailableReason::Blocked
                    }
                    IndexerInfoResolutionError::HostResolutionFailed(err) => {
                        tracing::debug!(error=?err, "host resolution failed");

                        let reason = match err {
                            HostResolutionError::InvalidUrl(_) => "invalid indexer URL",
                            HostResolutionError::DnsResolutionError(_) => {
                                "indexer URL DNS resolution failed"
                            }
                            HostResolutionError::Timeout => {
                                "indexer URL DNS resolution failed (timeout)"
                            }
                        };
                        UnavailableReason::IndexerResolutionError(reason)
                    }
                    IndexerInfoResolutionError::BlockedByHostBlocklist => {
                        UnavailableReason::Blocked
                    }
                    IndexerInfoResolutionError::IndexerServiceVersionResolutionFailed(err) => {
                        tracing::debug!(error=?err, "indexer service version resolution failed");

                        let reason = match err {
                            VersionResolutionError::FetchError(_) => {
                                "indexer service version resolution failed"
                            }
                            VersionResolutionError::Timeout => {
                                "indexer service version resolution failed (timeout)"
                            }
                        };
                        UnavailableReason::IndexerResolutionError(reason)
                    }
                    IndexerInfoResolutionError::IndexerServiceVersionBelowMin(..) => {
                        UnavailableReason::IndexerServiceVersionBelowMin
                    }
                    IndexerInfoResolutionError::GraphNodeVersionResolutionFailed(err) => {
                        tracing::debug!(error=?err, "graph node version resolution failed");

                        let reason = match err {
                            VersionResolutionError::FetchError(_) => {
                                "graph node version resolution failed"
                            }
                            VersionResolutionError::Timeout => {
                                "graph node version resolution failed (timeout)"
                            }
                        };
                        UnavailableReason::IndexerResolutionError(reason)
                    }
                    IndexerInfoResolutionError::GraphNodeVersionBelowMin(..) => {
                        UnavailableReason::GraphNodeVersionBelowMin
                    }
                };
                ResolutionError::Unavailable(reason)
            }
            IndexingError::Indexing(err) => {
                let reason = match err {
                    IndexingInfoResolutionError::BlockedByPoiBlocklist => {
                        UnavailableReason::Blocked
                    }
                    IndexingInfoResolutionError::IndexingProgressNotFound => {
                        UnavailableReason::IndexingProgressNotFound
                    }
                };
                ResolutionError::Unavailable(reason)
            }
            IndexingError::Internal(reason) => ResolutionError::Internal(reason),
        }
    }
}

/// Indexing error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexingError {
    #[error(transparent)]
    Indexer(#[from] IndexerInfoResolutionError),

    #[error(transparent)]
    Indexing(#[from] IndexingInfoResolutionError),

    #[error("internal error: {0}")]
    Internal(&'static str),
}

/// Errors when processing the indexer information.
#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexerInfoResolutionError {
    /// The indexer was blocked by the addrProgressNotFoundess blocklist.
    #[error("indexer address blocked by blocklist")]
    BlockedByAddrBlocklist,

    /// The indexer's host resolution failed.
    #[error("indexer host resolution failed: {0}")]
    HostResolutionFailed(#[from] HostResolutionError),
    /// The indexer was blocked by the host blocklist.
    #[error("indexer host blocked by blocklist")]
    BlockedByHostBlocklist,

    /// The indexer's service version resolution failed.
    #[error("indexer service version resolution failed: {0}")]
    IndexerServiceVersionResolutionFailed(VersionResolutionError),
    /// The indexer's service version is below the minimum required.
    #[error("service version {0} below the minimum required {1}")]
    IndexerServiceVersionBelowMin(Version, Version),

    /// The indexer's graph node version resolution failed.
    #[error("graph node version resolution failed: {0}")]
    #[allow(dead_code)] // TODO: Remove once the graph node version requirement is enforced
    GraphNodeVersionResolutionFailed(VersionResolutionError),
    /// The indexer's graph node version is below the minimum required.
    #[error("graph node version {0} below the minimum required {1}")]
    GraphNodeVersionBelowMin(Version, Version),
}

/// Error when processing the indexer's indexing information.
#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexingInfoResolutionError {
    /// The indexing has been blocked by the public POIs blocklist.
    #[error("indexing blocked by POI blocklist")]
    BlockedByPoiBlocklist,

    /// The indexing progress information was not found.
    #[error("indexing progress information not found")]
    IndexingProgressNotFound,
}
