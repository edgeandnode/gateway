use alloy_primitives::{BlockHash, BlockNumber};
use indexer_selection::UnresolvedBlock;

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
