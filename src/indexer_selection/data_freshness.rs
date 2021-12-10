use crate::indexer_selection::{
    block_requirements::BlockRequirements,
    utility::{concave_utility, SelectionFactor},
    BadIndexerReason, SelectionError,
};

#[derive(Clone, Debug, Default)]
pub struct DataFreshness {
    blocks_behind: Option<u64>,
    highest_reported_block: Option<u64>,
}

impl DataFreshness {
    pub fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        self.blocks_behind
            .ok_or(BadIndexerReason::MissingIndexingStatus)
    }

    pub fn set_blocks_behind(&mut self, blocks: u64, highest: u64) {
        self.blocks_behind = Some(blocks);
        self.highest_reported_block = Some(highest)
    }

    pub fn observe_indexing_behind(&mut self, minimum_block: u64, latest: u64) {
        let blocks_behind = if let Some(blocks_behind) = self.blocks_behind {
            blocks_behind
        } else {
            return;
        };
        // They are at least one block behind the assumed status (this
        // will usually be the case). In some cases for timing issues
        // they may have already reported they are even farther behind,
        // so we assume the worst of the two.
        let min_behind = latest.saturating_sub(minimum_block) + 1;
        self.blocks_behind = Some(latest.min(blocks_behind.max(min_behind)));
    }

    pub fn expected_utility(
        &self,
        requirements: &BlockRequirements,
        u_a: f64,
        latest_block: u64,
        blocks_behind: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        // Require the Indexer to have synced at least up to the minimum block
        if let Some(minimum) = requirements.minimum_block {
            let our_latest = latest_block.saturating_sub(blocks_behind);
            if our_latest < minimum {
                return Err(BadIndexerReason::BehindMinimumBlock.into());
            }
        }
        // Add utility if the latest block is requested. Otherwise,
        // data freshness is not a utility, but a binary of minimum block.
        // (Note that it can be both).
        if requirements.has_latest {
            let utility = {
                if blocks_behind == 0 {
                    1.0
                } else {
                    let freshness = 1.0 / blocks_behind as f64;
                    concave_utility(freshness, u_a)
                }
            };
            Ok(SelectionFactor::one(utility))
        } else {
            Ok(SelectionFactor::zero())
        }
    }
}
