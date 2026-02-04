//! Single Chain Head Tracking
//!
//! Tracks recent blocks for a single blockchain, determining consensus among indexers.
//!
//! # Consensus Algorithm
//!
//! The [`Chain::consensus_blocks`] method returns blocks with simple majority consensus:
//! - At each block number, count indexers reporting each block hash
//! - If one hash has strictly more indexers than all other hashes at that number,
//!   it's considered the consensus block
//! - Used to determine the "true" chain head among potentially forking indexers
//!
//! # Block Storage
//!
//! - Stores up to 512 blocks ([`MAX_LEN`])
//! - When full, evicts blocks with the lowest block number
//! - Each block tracks which indexers reported it
//!
//! # Block Production Rate
//!
//! The [`Chain::blocks_per_minute`] method estimates block production rate from
//! the timestamp difference between oldest and newest consensus blocks.
//! Defaults to 6 blocks/minute if insufficient data.

use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
};

use thegraph_core::{IndexerId, alloy::primitives::BlockHash};

use crate::blocks::Block;

#[derive(Default)]
pub struct Chain(BTreeMap<Block, BTreeSet<IndexerId>>);

const MAX_LEN: usize = 512;
const DEFAULT_BLOCKS_PER_MINUTE: u64 = 6;

impl Chain {
    pub fn latest(&self) -> Option<&Block> {
        self.consensus_blocks().next()
    }

    pub fn find(&self, unresolved: &BlockHash) -> Option<&Block> {
        self.consensus_blocks().find(|b| &b.hash == unresolved)
    }

    /// Return the average block production rate, based on the consensus blocks. The result will
    /// be greater than 0.
    pub fn blocks_per_minute(&self) -> u64 {
        let mut blocks = self.consensus_blocks();
        let last = blocks.next();
        let first = blocks.last();
        let (first, last) = match (first, last) {
            (Some(first), Some(last)) => (first, last),
            _ => return DEFAULT_BLOCKS_PER_MINUTE,
        };
        let b = last.number.saturating_sub(first.number).max(1);
        let t = last.timestamp.saturating_sub(first.timestamp).max(1);
        let bps = b as f64 / t as f64;
        (bps * 60.0) as u64
    }

    pub fn should_insert(&self, block: &Block, indexer: &IndexerId) -> bool {
        let redundant = self
            .0
            .get(block)
            .map(|indexers| indexers.contains(indexer))
            .unwrap_or(false);
        let lowest_block = self.0.first_key_value().map(|(b, _)| b.number).unwrap_or(0);
        let has_space = (self.0.len() < MAX_LEN) || (block.number > lowest_block);
        !redundant && has_space
    }

    pub fn insert(&mut self, block: Block, indexer: IndexerId) {
        tracing::trace!(%indexer, ?block);
        debug_assert!(self.should_insert(&block, &indexer));
        if self.0.len() >= MAX_LEN {
            self.evict();
        }
        self.0.entry(block).or_default().insert(indexer);
    }

    /// Remove all entries associated with the lowest block number.
    fn evict(&mut self) {
        let min_block = match self.0.pop_first() {
            Some((min_block, _)) => min_block,
            None => return,
        };
        while let Some(entry) = self.0.first_entry() {
            debug_assert!(entry.key().number >= min_block.number);
            if entry.key().number > min_block.number {
                break;
            }
            entry.remove();
        }
    }

    /// Return blocks with simple majority consensus, starting from the latest block.
    pub fn consensus_blocks(&self) -> impl Iterator<Item = &Block> {
        struct ConsensusBlocks<Iter> {
            blocks: Iter,
        }
        impl<'c, Iter> Iterator for ConsensusBlocks<iter::Peekable<Iter>>
        where
            Iter: Iterator<Item = (&'c Block, &'c BTreeSet<IndexerId>)> + Clone,
        {
            type Item = &'c Block;
            fn next(&mut self) -> Option<Self::Item> {
                loop {
                    let number = self.blocks.peek()?.0.number;
                    let forks = self.blocks.clone().take_while(|(b, _)| b.number == number);
                    let forks_len = forks.clone().count();
                    let max_indexers = forks.clone().map(|(_, i)| i.len()).max().unwrap();
                    let mut candidates = forks.clone().filter(|(_, i)| i.len() == max_indexers);
                    for _ in 0..forks_len {
                        self.blocks.next();
                    }
                    if candidates.clone().count() == 1 {
                        return candidates.next().map(|(b, _)| b);
                    }
                }
            }
        }
        ConsensusBlocks {
            blocks: self.0.iter().rev().peekable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::{Rng as _, RngCore as _, SeedableRng, rngs::SmallRng, seq::IndexedRandom as _};
    use thegraph_core::{
        IndexerId,
        alloy::primitives::{Address, BlockHash, U256},
    };

    use super::{Block, Chain, MAX_LEN};
    use crate::concat_bytes;

    #[test]
    fn chain() {
        let mut chain: Chain = Default::default();
        let indexers: Vec<IndexerId> = (1..=3)
            .map(|n| Address::from(concat_bytes!(20, [&[0; 19], &[n]])).into())
            .collect();
        let seed = rand::rng().next_u64();
        println!("seed: {seed}");
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut block_number: u64 = 0;
        let mut timestamp: u64 = 0;
        for _ in 0..(MAX_LEN * 2) {
            block_number += rng.random_range(0..=2);
            timestamp += rng.random_range(0..=1);
            let block = Block {
                number: block_number,
                hash: BlockHash::from(U256::from(timestamp)),
                timestamp,
            };
            let indexer = *indexers.choose(&mut rng).unwrap();
            if chain.should_insert(&block, &indexer) {
                chain.insert(block, indexer);
            }
        }

        // println!("{:#?}", chain.0);
        // println!("{:#?}", chain.consensus_blocks().collect::<Vec<_>>());

        assert!(chain.0.len() <= MAX_LEN, "chain len above max");
        assert!(chain.consensus_blocks().count() <= chain.0.len());
        assert!(chain.blocks_per_minute() > 0);
        let blocks = || chain.0.keys();
        assert!(
            blocks().tuple_windows().all(|(a, b)| a.number <= b.number),
            "chain block numbers not monotonic, check ord impl"
        );
        for block in chain.consensus_blocks() {
            let max_fork_indexers = chain
                .0
                .iter()
                .filter(|(block, _)| (block != block) && (block.number == block.number))
                .map(|(_, indexers)| indexers.len())
                .max()
                .unwrap_or(0);
            assert!(
                chain.0.get(block).unwrap().len() > max_fork_indexers,
                "consensus block without majority consensus"
            );
        }
    }
}
