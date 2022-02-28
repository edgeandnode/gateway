use crate::{
    indexer_selection::{BlockPointer, CostModelSource},
    prelude::test_utils::bytes_from_id,
    prelude::*,
};

pub const TEST_KEY: &'static str =
    "244226452948404D635166546A576E5A7234753778217A25432A462D4A614E64";

pub fn default_cost_model(price: GRT) -> CostModelSource {
    CostModelSource {
        model: format!("default => {};", price),
        globals: "".into(),
    }
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
