use std::collections::BTreeSet;

use alloy_primitives::BlockNumber;
use anyhow::anyhow;

use crate::{
    blocks::{BlockConstraint, UnresolvedBlock},
    chain::Chain,
    errors::Error,
};

#[derive(Debug)]
pub struct BlockRequirements {
    /// required block range, for exact block constraints (`number` & `hash`)
    pub range: Option<(BlockNumber, BlockNumber)>,
    /// maximum `number_gte` constraint
    pub number_gte: Option<BlockNumber>,
    /// does the query benefit from using the latest block (contains NumberGTE or Unconstrained)
    pub latest: bool,
}

pub fn resolve_block_requirements(
    chain: &Chain,
    constraints: &BTreeSet<BlockConstraint>,
    manifest_min_block: BlockNumber,
) -> Result<BlockRequirements, Error> {
    let latest = constraints.iter().any(|c| match c {
        BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => true,
        BlockConstraint::Hash(_) | BlockConstraint::Number(_) => false,
    });
    let number_gte = constraints
        .iter()
        .filter_map(|c| match c {
            BlockConstraint::NumberGTE(n) => Some(*n),
            _ => None,
        })
        .max();

    let exact_constraints: Vec<u64> = constraints
        .iter()
        .filter_map(|c| match c {
            BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => None,
            BlockConstraint::Number(number) => Some(*number),
            // resolving block hashes is not guaranteed
            BlockConstraint::Hash(hash) => chain
                .find(&UnresolvedBlock::WithHash(*hash))
                .map(|b| b.number),
        })
        .collect();
    let min_block = exact_constraints.iter().min().cloned();
    let max_block = exact_constraints.iter().max().cloned();

    // Reject queries for blocks before the minimum start block in the manifest, but only if the
    // constraint is for an exact block. For example, we always want to allow `block_gte: 0`.
    let request_contains_invalid_blocks = exact_constraints
        .iter()
        .any(|number| *number < manifest_min_block);
    if request_contains_invalid_blocks {
        return Err(Error::BadQuery(anyhow!(
            "requested block {}, before minimum `startBlock` of manifest {}",
            min_block.unwrap_or_default(),
            manifest_min_block,
        )));
    }

    Ok(BlockRequirements {
        range: min_block.map(|min| (min, max_block.unwrap())),
        number_gte,
        latest,
    })
}
