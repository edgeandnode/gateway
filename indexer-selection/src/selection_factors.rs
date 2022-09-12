use crate::{
    block_requirements::*, decay::DecayBuffer, performance::*, price_efficiency::*, reputation::*,
    utility::*, BadIndexerReason, Context, SelectionError,
};
use cost_model::CostModel;
use prelude::*;
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct SelectionFactors {
    status: IndexingStatus,
    performance: DecayBuffer<Performance>,
    reputation: DecayBuffer<Reputation>,
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
}

impl SelectionFactors {
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
        if latest_query_block <= status.reported_number {
            self.status.block = None;
            self.reputation.current_mut().penalize(130);
        } else {
            // They are at least one block behind the assumed status (this will usually be the
            // case). In some cases for timing issues they may have already reported they are even
            // farther behind, so we assume the worst of the two.
            status.blocks_behind = status.blocks_behind.max(blocks_behind + 1);
        }
    }

    pub fn penalize(&mut self, weight: u8) {
        self.reputation.current_mut().penalize(weight);
    }

    pub fn decay(&mut self) {
        self.performance.decay();
        self.reputation.decay();
    }

    pub fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        self.status
            .block
            .as_ref()
            .map(|s| s.blocks_behind)
            .ok_or(BadIndexerReason::MissingIndexingStatus)
    }

    pub fn expected_performance_utility(
        &self,
        utility_parameters: UtilityParameters,
    ) -> SelectionFactor {
        self.performance.expected_utility(utility_parameters)
    }

    pub fn expected_reputation_utility(
        &self,
        utility_parameters: UtilityParameters,
    ) -> SelectionFactor {
        self.reputation.expected_utility(utility_parameters)
    }

    pub fn expected_freshness_utility(
        &self,
        requirements: &BlockRequirements,
        utility_parameters: UtilityParameters,
        latest_block: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        let status = self
            .status
            .block
            .as_ref()
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        // Check that the indexer has synced at least up to any minimum block required.
        if let Some(minimum) = requirements.minimum_block {
            let indexer_latest = latest_block.saturating_sub(status.blocks_behind);
            if indexer_latest < minimum {
                return Err(BadIndexerReason::BehindMinimumBlock.into());
            }
        }
        // Add utility if the latest block is requested. Otherwise, data freshness is not a utility,
        // but a binary of minimum block. Note that it can be both.
        let utility = if !requirements.has_latest || (status.blocks_behind == 0) {
            1.0
        } else {
            let freshness = 1.0 / status.blocks_behind as f64;
            concave_utility(freshness, utility_parameters.a)
        };
        Ok(SelectionFactor {
            utility,
            weight: utility_parameters.weight,
        })
    }

    pub fn total_allocation(&self) -> GRT {
        self.status
            .allocations
            .iter()
            .map(|(_, size)| size)
            .fold(GRT::zero(), |sum, size| sum + *size)
    }

    pub fn get_price(
        &self,
        context: &mut Context<'_>,
        weight: f64,
        max_budget: &GRT,
    ) -> Result<(USD, SelectionFactor), SelectionError> {
        get_price(&self.status.cost_model, context, weight, max_budget)
    }
}
