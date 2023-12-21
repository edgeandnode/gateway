use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

use toolshed::url::Url;

use gateway_common::types::GRT;

use crate::decay::ISADecayBuffer;
use crate::performance::*;
use crate::reliability::*;
use crate::{BlockRequirements, IndexerErrorObservation};

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
    pub block: Option<BlockStatus>,
}

/// We compare candidate indexers based on their last reported block. Any observation of the indexer
/// behind the reported block will result in a penalty and the block status being considered
/// untrustworthy until the next time it is reported.
#[derive(Clone, Debug)]
pub struct BlockStatus {
    pub reported_number: u64,
    pub behind_reported_block: bool,
    pub min_block: Option<u64>,
}

impl BlockStatus {
    pub fn meets_requirements(&self, requirements: &BlockRequirements, block_rate_hz: f64) -> bool {
        if self.behind_reported_block {
            return false;
        }

        // Allow selecting indexers if their reported block is "close enough" to the required range. This is to allow an
        // admittedly fragile use-case where client queries contain a constraint based on the most recent block from
        // some RPC provider. Indexers closer to chain head and with higher success rate will be favored all else being
        // equal.
        let offset = (block_rate_hz * 60.0) as u64;

        let (min, max) = match requirements.range {
            Some((start, end)) => (start.saturating_sub(offset), end.saturating_sub(offset)),
            None => return true,
        };
        let reported_range = RangeInclusive::new(self.min_block.unwrap_or(0), self.reported_number);
        reported_range.contains(&min) && reported_range.contains(&max)
    }
}

impl IndexingState {
    pub fn update_status(&mut self, status: IndexingStatus) {
        self.status = status;
    }

    pub fn observe_query(
        &mut self,
        duration: Duration,
        result: Result<(), IndexerErrorObservation>,
    ) {
        self.last_use = self.last_use.max(Instant::now() - duration);
        match result {
            Ok(()) => self.perf_success.current_mut().observe(duration),
            Err(err) => {
                self.perf_failure.current_mut().observe(duration);
                match err {
                    IndexerErrorObservation::Other => (),
                    IndexerErrorObservation::Timeout | IndexerErrorObservation::BadAttestation => {
                        self.penalize()
                    }
                    IndexerErrorObservation::IndexingBehind { latest_query_block } => {
                        self.observe_indexing_behind(latest_query_block);
                        // Avoid negative impact on reliability score resulting from our predictions
                        // of the indexer's block status.
                        return;
                    }
                };
            }
        };
        self.reliability.current_mut().observe(result.is_ok());
    }

    fn observe_indexing_behind(&mut self, latest_query_block: u64) {
        let status = match &mut self.status.block {
            Some(status) => status,
            None => return,
        };
        if latest_query_block <= status.reported_number {
            status.behind_reported_block = true;
            self.penalize();
        }
    }

    pub fn penalize(&mut self) {
        self.reliability.current_mut().penalize();
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
}
