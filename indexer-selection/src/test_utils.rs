use std::hash::{Hash as _, Hasher as _};

use alloy_primitives::Address;
use siphasher::sip::SipHasher24;

use prelude::test_utils::bytes_from_id;
use prelude::GRT;
use toolshed::thegraph::DeploymentId;

use crate::{BlockPointer, CostModel};

pub const TEST_KEY: &str = "244226452948404D635166546A576E5A7234753778217A25432A462D4A614E64";

pub fn default_cost_model(fee: GRT) -> CostModel {
    CostModel::compile(format!("default => {fee};"), "").unwrap()
}

#[track_caller]
pub fn assert_within(value: f64, expected: f64, tolerance: f64) {
    let diff = (value - expected).abs();
    assert!(
        diff <= tolerance,
        "Expected value of {expected} +- {tolerance} but got {value} which is off by {diff}",
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

pub fn test_allocation_id(indexer: &Address, deployment: &DeploymentId) -> Address {
    let mut hasher = SipHasher24::default();
    indexer.hash(&mut hasher);
    deployment.hash(&mut hasher);
    bytes_from_id(hasher.finish() as usize).into()
}
