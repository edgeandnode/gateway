use crate::{
    indexer_selection::{BlockPointer, BlockResolver, CostModel, UnresolvedBlock},
    prelude::test_utils::bytes_from_id,
    prelude::*,
};
use async_trait::async_trait;

pub const TEST_KEY: &'static str =
    "244226452948404D635166546A576E5A7234753778217A25432A462D4A614E64";

pub fn default_cost_model(price: GRT) -> CostModel {
    CostModel::compile(format!("default => {};", price), "".into()).unwrap()
}

#[track_caller]
pub fn assert_within(value: f64, expected: f64, tolerance: f64) {
    let diff = (value - expected).abs();
    assert!(
        diff <= tolerance,
        "Expected value of {} +- {} but got {} which is off by {}",
        expected,
        tolerance,
        value,
        diff
    );
}

pub fn gen_blocks(numbers: &[u64]) -> Vec<BlockPointer> {
    numbers
        .iter()
        .map(|&number| BlockPointer {
            number,
            hash: bytes_from_id(number as usize).into(),
        })
        .collect()
}

#[derive(Clone)]
pub struct TestBlockResolver {
    blocks: Vec<BlockPointer>,
    skip_latest: usize,
}

impl TestBlockResolver {
    pub fn new(blocks: Vec<BlockPointer>) -> Self {
        Self {
            blocks,
            skip_latest: 0,
        }
    }
}

#[async_trait]
impl BlockResolver for TestBlockResolver {
    fn latest_block(&self) -> Option<BlockPointer> {
        let index = self.blocks.len().saturating_sub(1 + self.skip_latest);
        self.blocks.get(index).cloned()
    }

    fn skip_latest(&mut self, skip: usize) {
        self.skip_latest = skip;
    }

    async fn resolve_block(
        &self,
        unresolved: UnresolvedBlock,
    ) -> Result<BlockPointer, UnresolvedBlock> {
        self.blocks
            .iter()
            .find(|block| unresolved.matches(block))
            .cloned()
            .ok_or(unresolved)
    }
}
