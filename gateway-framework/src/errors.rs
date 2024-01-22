use std::collections::BTreeMap;

use axum::response::{IntoResponse, Response};
use itertools::Itertools;

use indexer_selection::UnresolvedBlock;

use crate::graphql;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Errors that should only occur in exceptional conditions.
    #[error("internal error: {0:#}")]
    Internal(anyhow::Error),
    /// Failed to authenticate or authorize the client request.
    #[error("auth error: {0:#}")]
    Auth(anyhow::Error),
    /// The subgraph chain does not exist or is not supported.
    #[error("chain not found: {0}")]
    ChainNotFound(String),
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
    #[error("bad indexers: {0}")]
    BadIndexers(IndexerErrors),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        graphql::error_response(self).into_response()
    }
}

pub struct IndexerErrors(Vec<IndexerError>);

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
    MissingBlock,
}

impl std::fmt::Debug for IndexerErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for IndexerErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0.iter().map(ToString::to_string).join(", "))
    }
}

impl IndexerErrors {
    /// Sort errors in order of descending occurrence for display.
    pub fn new<Errors: IntoIterator<Item = IndexerError>>(errors: Errors) -> Self {
        let errors = errors.into_iter().collect_vec();
        let mut occurrences: BTreeMap<&IndexerError, usize> = Default::default();
        for error in &errors {
            *occurrences.entry(error).or_insert(0) += 1;
        }
        // dedup
        let mut errors = occurrences.keys().map(|&e| e.clone()).collect_vec();
        errors.sort_unstable_by_key(|error| *occurrences.get(error).unwrap());
        errors.reverse();
        Self(errors)
    }
}
