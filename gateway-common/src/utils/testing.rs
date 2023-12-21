use std::hash::{Hash as _, Hasher as _};
use std::io::{Cursor, Write as _};
use std::sync::Once;

use alloy_primitives::Address;
use siphasher::sip::SipHasher24;
use thegraph::types::{BlockPointer, DeploymentId};

use crate::utils::tracing::init_tracing;

pub const TEST_KEY: &str = "244226452948404D635166546A576E5A7234753778217A25432A462D4A614E64";

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

pub fn bytes_from_id<const N: usize>(id: usize) -> [u8; N] {
    let mut buf = [0u8; N];
    let mut cursor = Cursor::new(buf.as_mut());
    let _ = cursor.write(&id.to_le_bytes());
    buf
}

pub fn init_test_tracing() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| init_tracing(false))
}
