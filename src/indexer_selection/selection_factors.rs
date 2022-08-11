use crate::{
    indexer_selection::{
        allocations::*, block_requirements::*, data_freshness::*, decay::DecayBuffer,
        performance::*, price_efficiency::*, reputation::*, utility::*, BadIndexerReason, Context,
        IndexerError, SelectionError,
    },
    prelude::*,
};
use cost_model::CostModel;
use secp256k1::SecretKey;

#[derive(Default)]
pub struct SelectionFactors {
    status: Option<IndexingStatus>,
    performance: DecayBuffer<Performance>,
    reputation: DecayBuffer<Reputation>,
    freshness: DataFreshness,
    allocations: Allocations,
}

impl SelectionFactors {
    pub fn status(&self) -> &Option<IndexingStatus> {
        &self.status
    }
    pub fn set_status(&mut self, status: IndexingStatus) {
        let behind = status.latest.saturating_sub(status.block);
        self.freshness.set_blocks_behind(behind, status.latest);
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct IndexingStatus {
    pub cost_model: Option<Ptr<CostModel>>,
    pub block: u64,
    pub latest: u64,
}

impl SelectionFactors {
    pub fn update_allocations(&mut self, signer: SecretKey, new_allocations: Vec<(Address, GRT)>) {
        let allocations = &mut self.allocations;
        // Remove allocations not present in new_allocations
        for old_allocation in allocations.allocation_ids() {
            if new_allocations.iter().all(|(id, _)| &old_allocation != id) {
                allocations.remove_allocation(&old_allocation);
            }
        }
        // Add new_allocations not present in allocations
        for (id, size) in new_allocations {
            if !allocations.contains_allocation(&id) {
                allocations.add_allocation(id, signer.clone(), size);
            }
        }
    }

    // TODO: (Zac) receipts require special handling
    pub fn observe_successful_query(&mut self, duration: Duration, receipt: &[u8]) {
        self.performance.current_mut().add_query(duration, Ok(()));
        self.reputation.current_mut().add_successful_query();
        self.allocations.release(receipt, QueryStatus::Success);
    }

    pub fn observe_failed_query(
        &mut self,
        duration: Duration,
        receipt: &[u8],
        error: &IndexerError,
    ) {
        self.performance.current_mut().add_query(duration, Err(()));
        self.reputation.current_mut().add_failed_query();
        let status = match error {
            // The indexer is potentially unaware that it failed, since it may have sent a response
            // back with an attestation.
            IndexerError::Timeout => QueryStatus::Unknown,
            IndexerError::NoAttestation
            | IndexerError::UnattestableError
            | IndexerError::UnexpectedPayload
            | IndexerError::UnresolvedBlock
            | IndexerError::Other(_) => QueryStatus::Failure,
        };
        self.allocations.release(receipt, status);
        if error.is_timeout() {
            self.reputation.current_mut().penalize(50);
        }
    }

    pub fn observe_indexing_behind(&self, minimum_block: Option<u64>, latest: u64) {
        match minimum_block {
            Some(minimum_block) => self
                .freshness
                .observe_indexing_behind(minimum_block, latest),
            // The only way to reach this would be if they returned that the block was unknown or
            // not indexed for a query with an empty selection set.
            None => self.reputation.current_mut().penalize(130),
        };
    }

    pub fn penalize(&self, weight: u8) {
        self.reputation.current_mut().penalize(weight);
    }

    pub fn decay(&mut self) {
        self.performance.decay();
        self.reputation.decay();
    }

    pub fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        self.freshness.blocks_behind()
    }

    pub fn commit(&self, fee: &GRT) -> Result<Receipt, BorrowFail> {
        self.allocations.commit(fee)
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
        self.allocations.total_allocation()
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
