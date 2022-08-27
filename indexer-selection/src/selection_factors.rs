use crate::{
    block_requirements::*, data_freshness::*, decay::DecayBuffer, performance::*,
    price_efficiency::*, reputation::*, utility::*, BadIndexerReason, Context, SelectionError,
};
use cost_model::CostModel;
use prelude::*;
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct SelectionFactors {
    status: Option<IndexingStatus>,
    performance: DecayBuffer<Performance>,
    reputation: DecayBuffer<Reputation>,
    freshness: DataFreshness,
}

#[derive(Clone, Debug)]
pub struct IndexingStatus {
    pub allocations: Arc<HashMap<Address, GRT>>,
    pub cost_model: Option<Ptr<CostModel>>,
    pub block: u64,
    pub latest: u64,
}

impl SelectionFactors {
    pub fn set_status(&mut self, status: IndexingStatus) {
        let behind = status.latest.saturating_sub(status.block);
        self.freshness.set_blocks_behind(behind, status.latest);
        self.status = Some(status);
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

    pub fn observe_indexing_behind(&mut self, minimum_block: Option<u64>, latest: u64) {
        match minimum_block {
            Some(minimum_block) => self
                .freshness
                .observe_indexing_behind(minimum_block, latest),
            // The only way to reach this would be if they returned that the block was unknown or
            // not indexed for a query with an empty selection set.
            None => self.reputation.current_mut().penalize(130),
        };
    }

    pub fn penalize(&mut self, weight: u8) {
        self.reputation.current_mut().penalize(weight);
    }

    pub fn decay(&mut self) {
        self.performance.decay();
        self.reputation.decay();
    }

    pub fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        self.freshness.blocks_behind()
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
        freshness_requirements: &BlockRequirements,
        utility_parameters: UtilityParameters,
        latest_block: u64,
        blocks_behind: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        self.freshness.expected_utility(
            freshness_requirements,
            utility_parameters,
            latest_block,
            blocks_behind,
        )
    }

    pub fn total_allocation(&self) -> GRT {
        let status = match &self.status {
            Some(status) => status,
            None => return GRT::zero(),
        };
        status
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
        let cost_model = self
            .status
            .as_ref()
            .and_then(|stat| stat.cost_model.clone());
        get_price(&cost_model, context, weight, max_budget)
    }
}
