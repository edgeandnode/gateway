use crate::{
    indexer_selection::{
        allocations::*, block_requirements::*, data_freshness::*, decay::DecayBuffer,
        performance::*, price_efficiency::*, reputation::*, utility::*, BadIndexerReason, Context,
        IndexerError, SecretKey, SelectionError,
    },
    prelude::*,
};
use eventuals::EventualExt;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SelectionFactors {
    price_efficiency: PriceEfficiency,
    locked: Arc<RwLock<LockedState>>,
}

#[derive(Default)]
struct LockedState {
    performance: DecayBuffer<Performance>,
    reputation: DecayBuffer<Reputation>,
    freshness: DataFreshness,
    allocations: Allocations,
}

pub struct IndexingData {
    pub cost_model: EventualWriter<CostModelSource>,
    pub status: EventualWriter<IndexingStatus>,
    locked: Arc<RwLock<LockedState>>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct IndexingStatus {
    pub block: u64,
    pub latest: u64,
}

impl Reader for SelectionFactors {
    type Writer = IndexingData;
    fn new() -> (Self::Writer, Self) {
        let (cost_model_writer, cost_model) = Eventual::new();
        let locked = Arc::new(RwLock::default());
        let reader = Self {
            price_efficiency: PriceEfficiency::new(cost_model),
            locked: locked.clone(),
        };
        let (status_writer, status) = Eventual::new();
        let writer = Self::Writer {
            cost_model: cost_model_writer,
            status: status_writer,
            locked: locked.clone(),
        };
        status
            .pipe_async(move |status| {
                let locked = locked.clone();
                async move {
                    let mut lock = locked.write().await;
                    let behind = status.latest.saturating_sub(status.block);
                    lock.freshness.set_blocks_behind(behind, status.latest);
                }
            })
            .forever();
        (writer, reader)
    }
}

impl IndexingData {
    pub async fn add_allocation(&self, allocation_id: Address, secret: SecretKey, size: GRT) {
        let mut lock = self.locked.write().await;
        lock.allocations.add_allocation(allocation_id, secret, size);
    }

    pub async fn remove_allocation(&self, allocation_id: &Address, size: GRT) {
        let mut lock = self.locked.write().await;
        lock.allocations.remove_allocation(allocation_id, size);
    }

    pub async fn total_allocation(&self) -> GRT {
        let lock = self.locked.read().await;
        lock.allocations.total_allocation
    }
}

impl SelectionFactors {
    pub async fn observe_successful_query(&self, duration: Duration, receipt: &[u8]) {
        let mut lock = self.locked.write().await;
        lock.performance.current_mut().add_query(duration);
        lock.reputation.current_mut().add_successful_query();
        lock.allocations.release(receipt, QueryStatus::Success);
    }

    pub async fn observe_failed_query(
        &self,
        duration: Duration,
        receipt: &[u8],
        error: &IndexerError,
    ) {
        let mut lock = self.locked.write().await;
        lock.performance.current_mut().add_query(duration);
        lock.reputation.current_mut().add_failed_query();
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
        lock.allocations.release(receipt, status);
        if error.is_timeout() {
            lock.reputation.current_mut().penalize(50);
        }
    }

    pub async fn observe_indexing_behind(&self, minimum_block: Option<u64>, latest: u64) {
        let mut lock = self.locked.write().await;
        match minimum_block {
            Some(minimum_block) => lock
                .freshness
                .observe_indexing_behind(minimum_block, latest),
            // The only way to reach this would be if they returned that the block was unknown or
            // not indexed for a query with an empty selection set.
            None => lock.reputation.current_mut().penalize(130),
        };
    }

    pub async fn penalize(&self, weight: u8) {
        let mut lock = self.locked.write().await;
        lock.reputation.current_mut().penalize(weight);
    }

    pub async fn decay(&self) {
        let mut lock = self.locked.write().await;
        lock.performance.decay();
        lock.reputation.decay();
    }

    pub async fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        let lock = self.locked.read().await;
        lock.freshness.blocks_behind()
    }

    pub async fn commit(&self, fee: &GRT) -> Result<Receipt, BorrowFail> {
        let mut lock = self.locked.write().await;
        lock.allocations.commit(fee)
    }

    pub async fn expected_performance_utility(
        &self,
        utility_parameters: UtilityParameters,
    ) -> SelectionFactor {
        let lock = self.locked.read().await;
        lock.performance.expected_utility(utility_parameters)
    }

    pub async fn expected_reputation_utility(
        &self,
        utility_parameters: UtilityParameters,
    ) -> SelectionFactor {
        let lock = self.locked.read().await;
        lock.reputation.expected_utility(utility_parameters)
    }

    pub async fn expected_freshness_utility(
        &self,
        freshness_requirements: &BlockRequirements,
        utility_parameters: UtilityParameters,
        latest_block: u64,
        blocks_behind: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        let lock = self.locked.read().await;
        lock.freshness.expected_utility(
            freshness_requirements,
            utility_parameters,
            latest_block,
            blocks_behind,
        )
    }

    pub async fn total_allocation(&self) -> GRT {
        let lock = self.locked.read().await;
        lock.allocations.total_allocation
    }

    pub async fn get_price(
        &self,
        context: &mut Context<'_>,
        weight: f64,
        max_budget: &GRT,
    ) -> Result<(USD, SelectionFactor), SelectionError> {
        self.price_efficiency
            .get_price(context, weight, max_budget)
            .await
    }
}
