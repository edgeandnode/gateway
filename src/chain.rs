use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
    time::Duration,
};

use thegraph_core::{BlockHash, IndexerId};
use tokio::time::Instant;

use crate::blocks::Block;

#[derive(Default)]
pub struct Chain {
    blocks: BTreeMap<Block, BTreeSet<IndexerId>>,
    update: parking_lot::Mutex<Option<Instant>>,
}

const MAX_LEN: usize = 512;
const DEFAULT_BLOCKS_PER_MINUTE: u64 = 6;
const UPDATE_INTERVAL: Duration = Duration::from_secs(1);

impl Chain {
    pub fn update_ticket(&self) -> Option<()> {
        let mut update = self.update.try_lock()?;
        if matches!(*update, Some(t) if t.elapsed() < UPDATE_INTERVAL) {
            return None;
        }
        *update = Some(Instant::now());
        Some(())
    }

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

    pub fn insert(&mut self, block: Block, indexer: IndexerId) {
        tracing::trace!(%indexer, ?block);
        if !self.should_insert(&block, &indexer) {
            return;
        }
        if self.blocks.len() >= MAX_LEN {
            self.evict();
        }
        self.blocks.entry(block).or_default().insert(indexer);
    }

    fn should_insert(&self, block: &Block, indexer: &IndexerId) -> bool {
        let redundant = self
            .blocks
            .get(block)
            .map(|indexers| indexers.contains(indexer))
            .unwrap_or(false);
        let lowest_block = self
            .blocks
            .first_key_value()
            .map(|(b, _)| b.number)
            .unwrap_or(0);
        let has_space = (self.blocks.len() < MAX_LEN) || (block.number > lowest_block);
        !redundant && has_space
    }

    /// Remove all entries associated with the lowest block number.
    fn evict(&mut self) {
        let min_block = match self.blocks.pop_first() {
            Some((min_block, _)) => min_block,
            None => return,
        };
        while let Some(entry) = self.blocks.first_entry() {
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
            blocks: self.blocks.iter().rev().peekable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use itertools::Itertools as _;
    use rand::{
        rngs::SmallRng, seq::SliceRandom as _, thread_rng, Rng as _, RngCore as _, SeedableRng,
    };
    use thegraph_core::{Address, BlockHash, IndexerId};
    use toolshed::concat_bytes;

    use super::{Block, Chain, MAX_LEN};

    #[test]
    fn chain() {
        let mut chain: Chain = Default::default();
        let indexers: Vec<IndexerId> = (1..=3)
            .map(|n| Address::from(concat_bytes!(20, [&[0; 19], &[n]])).into())
            .collect();
        let seed = thread_rng().next_u64();
        println!("seed: {seed}");
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut block_number: u64 = 0;
        let mut timestamp: u64 = 0;
        for _ in 0..(MAX_LEN * 2) {
            block_number += rng.gen_range(0..=2);
            timestamp += rng.gen_range(0..=1);
            let block = Block {
                number: block_number,
                hash: BlockHash::from(U256::from(timestamp)),
                timestamp,
            };
            let indexer = *indexers.choose(&mut rng).unwrap();
            chain.insert(block, indexer);
        }

        // println!("{:#?}", chain.blocks);
        // println!("{:#?}", chain.consensus_blocks().collect::<Vec<_>>());

        assert!(chain.blocks.len() <= MAX_LEN, "chain len above max");
        assert!(chain.consensus_blocks().count() <= chain.blocks.len());
        assert!(chain.blocks_per_minute() > 0);
        let blocks = || chain.blocks.keys();
        assert!(
            blocks().tuple_windows().all(|(a, b)| a.number <= b.number),
            "chain block numbers not monotonic, check ord impl"
        );
        for block in chain.consensus_blocks() {
            let max_fork_indexers = chain
                .blocks
                .iter()
                .filter(|(block, _)| (block != block) && (block.number == block.number))
                .map(|(_, indexers)| indexers.len())
                .max()
                .unwrap_or(0);
            assert!(
                chain.blocks.get(block).unwrap().len() > max_fork_indexers,
                "consensus block without majority consensus"
            );
        }
    }
}
