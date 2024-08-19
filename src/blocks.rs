use std::fmt;

use serde::Deserialize;
use thegraph_core::{BlockHash, BlockNumber, BlockTimestamp};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub struct Block {
    pub number: BlockNumber,
    pub hash: BlockHash,
    pub timestamp: BlockTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum UnresolvedBlock {
    WithHash(BlockHash),
    WithNumber(BlockNumber),
}

impl UnresolvedBlock {
    pub fn matches(&self, block: &Block) -> bool {
        match self {
            Self::WithHash(hash) => hash == &block.hash,
            Self::WithNumber(number) => number == &block.number,
        }
    }
}

impl fmt::Display for UnresolvedBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithHash(hash) => write!(f, "{hash}"),
            Self::WithNumber(number) => write!(f, "{number}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum BlockConstraint {
    Unconstrained,
    Hash(BlockHash),
    Number(BlockNumber),
    NumberGTE(BlockNumber),
}

impl BlockConstraint {
    pub fn into_unresolved(self) -> Option<UnresolvedBlock> {
        match self {
            Self::Unconstrained => None,
            Self::Hash(h) => Some(UnresolvedBlock::WithHash(h)),
            Self::Number(n) | Self::NumberGTE(n) => Some(UnresolvedBlock::WithNumber(n)),
        }
    }
}
