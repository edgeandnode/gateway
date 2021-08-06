mod economic_security;
mod network_cache;
mod performance;
mod price_efficiency;
mod utility;

#[cfg(test)]
mod test_utils;

use crate::prelude::*;
use cost_model;
use economic_security::NetworkParameters;
use network_cache::NetworkCache;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectionError {
    BadInput,
    MissingBlock(UnresolvedBlock),
    BadIndexer(BadIndexerReason),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BadIndexerReason {
    MissingIndexerStake,
    MissingIndexerDelegatedStake,
    BehindMinimumBlock,
    MissingIndexingStatus,
    MissingCostModel,
    QueryNotCosted,
    FeeTooHigh,
    InsufficientCollateral,
    NaN,
}

impl From<BadIndexerReason> for SelectionError {
    fn from(err: BadIndexerReason) -> Self {
        Self::BadIndexer(err)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum UnresolvedBlock {
    WithNumber(u64),
    WithHash(Bytes32),
}

impl From<UnresolvedBlock> for SelectionError {
    fn from(err: UnresolvedBlock) -> Self {
        Self::MissingBlock(err)
    }
}

#[derive(Clone)]
pub struct IndexerSelection {
    network_params: Arc<RwLock<NetworkParameters>>,
    network_cache: Arc<RwLock<NetworkCache>>,
}
