use std::collections::BTreeMap;

use alloy_primitives::{Address, BlockNumber};
use axum::response::{IntoResponse, Response};

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
    /// Indexers are available, but failed to return a suitable result. The indexer errors are displayed in descending
    /// order of how many of the potential indexers failed for that reason.
    #[error("bad indexers: {0:?}")]
    BadIndexers(BTreeMap<Address, IndexerError>),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        graphql::error_response(self).into_response()
    }
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnavailableReason {
    /// Failed to query indexer version or indexing status for the requested deployment(s).
    #[error("no status")]
    NoStatus,
    /// The indexer has zero stake.
    #[error("no stake")]
    NoStake,
    /// The indexer's cost model did not produce a fee for the GraphQL document.
    #[error("no fee")]
    NoFee,
    /// The indexer did not have a block required by the query.
    #[error("missing block")]
    MissingBlock(MissingBlockError),
}

#[derive(Clone, Debug, PartialOrd, Ord)]
pub struct MissingBlockError {
    pub missing: Option<BlockNumber>,
    pub latest: Option<BlockNumber>,
}

impl PartialEq for MissingBlockError {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for MissingBlockError {}
