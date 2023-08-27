use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use eventuals::Ptr;
use prelude::GRT;
use semver::Version;
use toolshed::url::Url;

use crate::{
    decay::ISADecayBuffer, fee::indexer_fee, performance::*, reliability::*, BlockRequirements,
    Context, CostModel, IndexerErrorObservation, SelectionError,
};

pub struct IndexingState {
    pub status: IndexingStatus,
    pub reliability: ISADecayBuffer<Reliability>,
    pub perf_success: ISADecayBuffer<Performance>,
    pub perf_failure: ISADecayBuffer<Performance>,
    pub last_use: Instant,
}

impl IndexingState {
    pub fn new(status: IndexingStatus) -> Self {
        Self {
            status,
            reliability: ISADecayBuffer::default(),
            perf_success: ISADecayBuffer::default(),
            perf_failure: ISADecayBuffer::default(),
            last_use: Instant::now() - Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexingStatus {
    pub url: Url,
    pub stake: GRT,
    pub allocation: GRT,
    pub cost_model: Option<Ptr<CostModel>>,
    pub block: Option<BlockStatus>,
    pub version: Option<Arc<Version>>,
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
    pub fn update_status(&mut self, mut status: IndexingStatus) {
        // As long as we haven't witnessed the indexer behind a reported block height, take the best
        // value of `blocks_behind`. This is especially important for fast-moving chains to avoid
        // indexers being thrown much further behind without any observation to justify that.
        match (&self.status.block, &mut status.block) {
            (Some(prev), Some(next)) if !prev.behind_reported_block => {
                next.blocks_behind = next.blocks_behind.min(prev.blocks_behind);
            }
            _ => (),
        };

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
                    IndexerErrorObservation::Timeout | IndexerErrorObservation::BadAttestation => {
                        self.reliability.current_mut().penalize(30)
                    }
                    IndexerErrorObservation::IndexingBehind {
                        latest_query_block,
                        latest_block,
                    } => self.observe_indexing_behind(latest_query_block, latest_block),
                };
            }
        };
    }

    fn observe_indexing_behind(&mut self, latest_query_block: u64, latest_block: u64) {
        let status = match &mut self.status.block {
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
