use crate::{
    decay::ISADecayBuffer, fee::indexer_fee, performance::*, reliability::*, BlockRequirements,
    Context, CostModel, IndexerErrorObservation, SelectionError,
};
use prelude::*;
use std::{collections::HashMap, sync::Arc};

pub struct IndexingState {
    pub status: IndexingStatus,
    pub reliability: ISADecayBuffer<Reliability>,
    pub perf_success: ISADecayBuffer<Performance>,
    pub perf_failure: ISADecayBuffer<Performance>,
    pub last_use: Instant,
}

impl Default for IndexingState {
    fn default() -> Self {
        Self {
            status: IndexingStatus::default(),
            reliability: ISADecayBuffer::default(),
            perf_success: ISADecayBuffer::default(),
            perf_failure: ISADecayBuffer::default(),
            last_use: Instant::now() - Duration::from_secs(60),
        }
    }
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
    pub min_block: Option<u64>,
}

impl BlockStatus {
    pub fn meets_requirements(&self, requirements: &BlockRequirements, latest_block: u64) -> bool {
        let (min, max) = match requirements.range {
            Some(range) => range,
            None => return true,
        };
        let min_block = self.min_block.unwrap_or(0);
        let expected_block_status = latest_block.saturating_sub(self.blocks_behind);
        (min_block <= min) && (max <= expected_block_status)
    }
}

impl IndexingState {
    pub fn set_status(&mut self, status: IndexingStatus) {
        self.status = status;
    }

    pub fn observe_query(
        &mut self,
        duration: Duration,
        result: Result<(), IndexerErrorObservation>,
    ) {
        self.last_use = self.last_use.max(Instant::now() - duration);
        self.reliability.current_mut().observe(result.is_ok());
        match result {
            Ok(()) => self.perf_success.current_mut().observe(duration),
            Err(err) => {
                self.perf_failure.current_mut().observe(duration);
                match err {
                    IndexerErrorObservation::Other => (),
                    IndexerErrorObservation::Timeout => self.reliability.current_mut().penalize(50),
                    IndexerErrorObservation::IndexingBehind {
                        latest_query_block,
                        latest_block,
                    } => self.observe_indexing_behind(latest_query_block, latest_block),
                };
            }
        };
    }

    fn observe_indexing_behind(&mut self, latest_query_block: u64, latest_block: u64) {
        let mut status = match &mut self.status.block {
            Some(status) => status,
            None => return,
        };
        let blocks_behind = match latest_block.checked_sub(latest_query_block) {
            Some(blocks_behind) => blocks_behind,
            None => return,
        };
        if (latest_query_block <= status.reported_number) && !status.behind_reported_block {
            self.reliability.current_mut().penalize(130);
            // Only apply this harsh penaly once, until the reported status is updated.
            status.behind_reported_block = true;
        }
        // They are at least one block behind the assumed status (this will usually be the case).
        // In some cases for timing issues they may have already reported they are even farther
        // behind, so we assume the worst of the two.
        status.blocks_behind = status.blocks_behind.max(blocks_behind + 1);
    }

    pub fn penalize(&mut self, weight: u8) {
        self.reliability.current_mut().penalize(weight);
    }

    pub fn decay(&mut self) {
        let Self {
            status: _,
            last_use: _,
            perf_success,
            reliability,
            perf_failure,
        } = self;
        reliability.decay();
        perf_success.decay();
        perf_failure.decay();
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
