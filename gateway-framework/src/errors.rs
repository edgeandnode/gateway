use std::{
    collections::BTreeMap,
    fmt::{self, Write as _},
};

use alloy_primitives::{Address, BlockNumber};
use axum::response::{IntoResponse, Response};
use itertools::Itertools as _;

use crate::{blocks::UnresolvedBlock, graphql};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Errors that should only occur in exceptional conditions.
    #[error("internal error: {0:#}")]
    Internal(anyhow::Error),
    /// Failed to authenticate or authorize the client request.
    #[error("auth error: {0:#}")]
    Auth(anyhow::Error),
    /// A block required by the query is not found.
    #[error("block not found: {0}")]
    BlockNotFound(UnresolvedBlock),
    /// The requested subgraph or deployment is not found or invalid.
    #[error("subgraph not found: {0:#}")]
    SubgraphNotFound(anyhow::Error),
    /// The GraphQL query is invalid.
    #[error("bad query: {0:#}")]
    BadQuery(anyhow::Error),
    /// There are no indexers allocated to the requested subgraph or deployment.
    #[error("no indexers found")]
    NoIndexers,
    /// Indexers are available, but failed to return a suitable result.
    #[error("bad indexers: {0}")]
    BadIndexers(IndexerErrors),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        graphql::error_response(self).into_response()
    }
}

#[derive(Debug, Clone, Default)]
pub struct IndexerErrors(BTreeMap<Address, IndexerError>);

impl std::ops::Deref for IndexerErrors {
    type Target = BTreeMap<Address, IndexerError>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for IndexerErrors {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Display for IndexerErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self.iter().map(|(k, v)| format!("{k:?}: {v}")).join(", ");
        write!(f, "{{{}}}", entries)
    }
}

#[derive(thiserror::Error, Clone, Debug)]
pub enum IndexerError {
    /// Errors that should only occur in exceptional conditions.
    #[error("InternalError({0})")]
    Internal(&'static str),
    /// The indexer is considered unavailable.
    #[error("Unavailable({0})")]
    Unavailable(UnavailableReason),
    /// The indexer request timed out.
    #[error("Timeout")]
    Timeout,
    /// The indexer’s response is bad.
    #[error("BadResponse({0:#})")]
    BadResponse(String),
}

#[derive(thiserror::Error, Clone, Debug)]
pub enum UnavailableReason {
    /// The indexer is blocked by one of the block lists.
    #[error("blocked")]
    Blocked,

    /// The indexer deployment was blocked since it reported a bad POI blocked by the gateway.
    #[error("blocked: bad POI")]
    BlockedBadPOIs,

    /// The indexer version is not supported (e.g., the indexer service version is below the
    /// minimum version required by the gateway, etc.)
    #[error("not supported: {0}")]
    NotSupported(String),

    /// The indexer information resolution failed (e.g. the indexer failed to report the indexer
    /// version within the expected time, the indexer failed to report the indexing progress info
    /// within the expected time, etc.)
    #[error("no status: {0}")]
    NoStatus(String),

    /// The indexer has zero stake.
    #[error("no stake")]
    NoStake,

    /// The indexer's cost model did not produce a fee for the GraphQL document.
    #[error("no fee")]
    NoFee,

    /// The indexer did not have a block required by the query.
    #[error("{}", .0.message())]
    MissingBlock(MissingBlockError),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MissingBlockError {
    pub missing: Option<BlockNumber>,
    pub latest: Option<BlockNumber>,
}

impl MissingBlockError {
    fn message(&self) -> String {
        let mut text = "missing block".to_string();
        if let Some(n) = self.missing {
            write!(&mut text, ": {n}").unwrap();
        }
        if let Some(n) = self.latest {
            write!(&mut text, ", latest: {n}").unwrap();
        }
        text
    }
}
