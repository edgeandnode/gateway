use crate::{
  indexer_selection::{utility::SelectionFactor, BlockPointer},
  prelude::test_utils::bytes_from_id,
};

pub const TEST_KEY: &str = "244226452948404D635166546A576E5A7234753778217A25432A462D4A614E64";

pub fn assert_within(utility: SelectionFactor, expected: f64, tolerance: f64) {
  let diff = (utility.utility - expected).abs();
  assert!(
    diff <= tolerance,
    "Expected utility of {} +- {} but got {} which is off by {}",
    expected,
    tolerance,
    utility.utility,
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
