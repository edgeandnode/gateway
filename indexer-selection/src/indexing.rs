use crate::{
    decay::DecayBuffer, performance::*, price_efficiency::indexer_fee, reliability::*, Context,
    CostModel, IndexerErrorObservation, SelectionError,
};
use prelude::*;
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct IndexingState {
    pub status: IndexingStatus,
    pub reliability: DecayBuffer<Reliability>,
    pub perf_success: DecayBuffer<Performance>,
    pub perf_failure: DecayBuffer<Performance>,
    pub last_use: Option<Instant>,
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

    pub fn observe_query(
        &mut self,
        duration: Duration,
        result: Result<(), IndexerErrorObservation>,
    ) {
        let t = Instant::now() - duration;
        self.last_use = Some(self.last_use.map(|t0| t0.max(t)).unwrap_or(t));
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
        self.reliability.decay();
        self.perf_success.decay();
        self.perf_failure.decay();
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
