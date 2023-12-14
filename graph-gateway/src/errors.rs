use std::collections::BTreeMap;

use indexer_selection::UnresolvedBlock;
use itertools::Itertools;

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
    /// The indexer’s fee is above the gateway’s budget.
    #[error("fee too high")]
    FeeTooHigh,
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
        let mut errors = errors.into_iter().collect_vec();
        let mut occurrences: BTreeMap<IndexerError, usize> = Default::default();
        for error in &errors {
            *occurrences.entry(error.clone()).or_insert(0) += 1;
        }
        errors.sort_unstable_by_key(|error| *occurrences.get(error).unwrap());
        errors.reverse();
        Self(errors)
    }
}