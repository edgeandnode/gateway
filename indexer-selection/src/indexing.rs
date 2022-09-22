use crate::{
    decay::DecayBuffer, performance::*, price_efficiency::indexer_fee, reputation::*, Context,
    CostModel, SelectionError,
};
use prelude::*;
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct IndexingState {
    pub status: IndexingStatus,
    pub performance: DecayBuffer<Performance>,
    pub reputation: DecayBuffer<Reputation>,
}

#[derive(Clone, Debug, Default)]
pub struct IndexingStatus {
    pub allocations: Arc<HashMap<Address, GRT>>,
    pub cost_model: Option<Ptr<CostModel>>,
    pub block: Option<BlockStatus>,
}

/// Indexers are expected to monotonically increase their block height on a deployment. We also
/// speculate that the indexer will remain the same amount of blocks behind chain head as it was the
/// last time it reported its status. The count of blocks behind will be adjusted based on indexer
/// responses at the blocks requested. Any observation of the indexer behind the reported block will
/// result in a penalty and the block status being cleared until the next time it is reported.
#[derive(Clone, Debug)]
pub struct BlockStatus {
    pub reported_number: u64,
    pub blocks_behind: u64,
    pub behind_reported_block: bool,
}

impl IndexingState {
    pub fn set_status(&mut self, status: IndexingStatus) {
        self.status = status;
    }

    pub fn observe_successful_query(&mut self, duration: Duration) {
        self.performance.current_mut().add_query(duration, Ok(()));
        self.reputation.current_mut().add_successful_query();
    }

    pub fn observe_failed_query(&mut self, duration: Duration, timeout: bool) {
        self.performance.current_mut().add_query(duration, Err(()));
        self.reputation.current_mut().add_failed_query();
        if timeout {
            self.reputation.current_mut().penalize(50);
        }
    }

    pub fn observe_indexing_behind(&mut self, latest_query_block: u64, latest_block: u64) {
        let mut status = match &mut self.status.block {
            Some(status) => status,
            None => return,
        };
        let blocks_behind = match latest_block.checked_sub(latest_query_block) {
            Some(blocks_behind) => blocks_behind,
            None => return,
        };
        if (latest_query_block <= status.reported_number) && !status.behind_reported_block {
            self.reputation.current_mut().penalize(130);
            // Only apply this harsh penaly once, until the reported status is
            // updated.
            status.behind_reported_block = true;
        }
        // They are at least one block behind the assumed status (this will usually be the case).
        // In some cases for timing issues they may have already reported they are even farther
        // behind, so we assume the worst of the two.
        status.blocks_behind = status.blocks_behind.max(blocks_behind + 1);
    }

    pub fn penalize(&mut self, weight: u8) {
        self.reputation.current_mut().penalize(weight);
    }

    pub fn decay(&mut self) {
        self.performance.decay();
        self.reputation.decay();
    }

    pub fn total_allocation(&self) -> GRT {
        self.status
            .allocations
            .iter()
            .map(|(_, size)| size)
            .fold(GRT::zero(), |sum, size| sum + *size)
    }

    pub fn fee(
        &self,
        context: &mut Context<'_>,
        weight: f64,
        budget: &GRT,
        max_indexers: u8,
    ) -> Result<GRT, SelectionError> {
        indexer_fee(
            &self.status.cost_model,
            context,
            weight,
            budget,
            max_indexers,
        )
    }
}
