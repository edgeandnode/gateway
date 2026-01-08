//! Block Type Definitions
//!
//! Core types for representing blockchain blocks and query constraints.

use serde::Deserialize;
use thegraph_core::alloy::primitives::{BlockHash, BlockNumber, BlockTimestamp};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub struct Block {
    pub number: BlockNumber,
    pub hash: BlockHash,
    pub timestamp: BlockTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum BlockConstraint {
    Unconstrained,
    Hash(BlockHash),
    Number(BlockNumber),
    NumberGTE(BlockNumber),
}
