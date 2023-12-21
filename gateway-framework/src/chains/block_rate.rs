use std::time::Duration;

use alloy_primitives::BlockNumber;
use itertools::Itertools;

/// Estimates blocks per minute, based on the past minute of chain head block heights.
pub struct Estimator {
    capacity: usize,
    block_heights: Vec<BlockNumber>,
}

impl Estimator {
    pub fn new(poll_interval: Duration) -> Self {
        let updates_per_minute = 60 / poll_interval.as_secs();
        let capacity = updates_per_minute as usize + 1;
        let block_heights = Vec::with_capacity(capacity);
        Self {
            capacity,
            block_heights,
        }
    }

    /// Expected to be called approximately once per `poll_interval`. Returns the updated block rate
    /// in blocks per minute.
    pub fn update(&mut self, block_height: BlockNumber) -> u64 {
        if self.block_heights.len() < self.capacity {
            self.block_heights.push(block_height);
        } else {
            self.block_heights.rotate_left(1);
            *self.block_heights.last_mut().unwrap() = block_height;
        }
        // Sum the last minute of block height deltas.
        self.block_heights
            .iter()
            .tuple_windows()
            .map(|(a, b)| b.saturating_sub(*a))
            .sum()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn estimator() {
        let mut estimator = Estimator::new(Duration::from_secs(20));
        assert_eq!(estimator.update(1), 0);
        assert_eq!(estimator.update(2), 1);
        assert_eq!(estimator.update(3), 2);
        assert_eq!(estimator.update(4), 3);
        assert_eq!(estimator.update(5), 3);
        assert_eq!(estimator.update(6), 3);
        assert_eq!(estimator.update(8), 4);
        assert_eq!(estimator.update(10), 5);
        assert_eq!(estimator.update(12), 6);
        assert_eq!(estimator.update(12), 4);
        assert_eq!(estimator.update(12), 2);
        assert_eq!(estimator.update(12), 0);
        assert_eq!(estimator.update(13), 1);
    }
}
