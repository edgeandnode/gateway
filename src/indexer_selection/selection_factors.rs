use crate::{
    indexer_selection::{
        network_cache::*, performance::*, price_efficiency::*, receipts::*, reputation::*,
        utility::*, BadIndexerReason, Context, Indexing, SecretKey, SelectionError,
    },
    prelude::*,
};
use eventuals::EventualExt;
use std::sync::Arc;
use tokio::{sync::RwLock, time};
use tree_buf::{Decode, Encode};

pub struct SelectionFactors {
    pub allocation: Eventual<GRT>,
    price_efficiency: PriceEfficiency,
    locked: Arc<RwLock<LockedState>>,
}

#[derive(Default)]
struct LockedState {
    performance: Performance,
    reputation: Reputation,
    freshness: DataFreshness,
    receipts: Receipts,
}

pub struct IndexingData {
    pub allocation: EventualWriter<GRT>,
    pub cost_model: EventualWriter<CostModelSource>,
    pub status: EventualWriter<IndexingStatus>,
    locked: Arc<RwLock<LockedState>>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct IndexingStatus {
    pub block: u64,
    pub latest: u64,
}

#[derive(Debug, Decode, Encode)]
pub struct IndexingSnapshot {
    pub allocation: Option<String>,
    pub cost_model: Option<CostModelSource>,
    pub indexing: Indexing,
    pub performance: Performance,
    pub freshness: DataFreshness,
    pub reputation: Reputation,
}

impl Reader for SelectionFactors {
    type Writer = IndexingData;
    fn new() -> (Self::Writer, Self) {
        let (allocation_writer, allocation) = Eventual::new();
        let (cost_model_writer, cost_model) = Eventual::new();
        let locked = Arc::new(RwLock::default());
        let reader = Self {
            allocation,
            price_efficiency: PriceEfficiency::new(cost_model),
            locked: locked.clone(),
        };
        let (status_writer, status) = Eventual::new();
        let writer = Self::Writer {
            allocation: allocation_writer,
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
    pub async fn add_allocation(&mut self, allocation_id: Address, size: &GRT, secret: SecretKey) {
        self.allocation
            .update(|prev| *prev.unwrap_or(&GRT::zero()) + *size);
        let mut lock = self.locked.write().await;
        lock.receipts.add_allocation(allocation_id, secret);
    }

    pub async fn remove_allocation(&mut self, allocation_id: &Address, size: &GRT) {
        self.allocation
            .update(|prev| prev.map(|v| v.saturating_sub(*size)).unwrap_or(GRT::zero()));
        let mut lock = self.locked.write().await;
        lock.receipts.remove_allocation(allocation_id);
    }

    pub async fn add_transfer(&self, transfer_id: Bytes32, collateral: &GRT, secret: SecretKey) {
        let mut lock = self.locked.write().await;
        lock.receipts.add_transfer(transfer_id, collateral, secret);
    }

    pub async fn remove_transfer(&self, transfer_id: &Bytes32) {
        let mut lock = self.locked.write().await;
        lock.receipts.remove_transfer(transfer_id);
    }
}

impl SelectionFactors {
    pub async fn add_transfer(&self, transfer_id: Bytes32, collateral: &GRT, secret: SecretKey) {
        let mut lock = self.locked.write().await;
        lock.receipts.add_transfer(transfer_id, collateral, secret);
    }

    pub async fn observe_successful_query(&self, duration: time::Duration, receipt: &[u8]) {
        let mut lock = self.locked.write().await;
        lock.performance.add_successful_query(duration);
        lock.reputation.add_successful_query();
        lock.receipts.release(receipt, QueryStatus::Success);
    }

    pub async fn observe_failed_query(&self, receipt: &[u8], status: QueryStatus) {
        let mut lock = self.locked.write().await;
        lock.reputation.add_failed_query();
        lock.receipts.release(receipt, status);
    }

    pub async fn observe_indexing_behind(
        &self,
        freshness_requirements: &Result<BlockRequirements, SelectionError>,
        latest: u64,
    ) {
        let mut lock = self.locked.write().await;
        lock.freshness
            .observe_indexing_behind(freshness_requirements, latest);
    }

    pub async fn decay(&self, retain: f64) {
        let mut lock = self.locked.write().await;
        lock.performance.decay(retain);
        lock.reputation.decay(retain);
    }

    pub async fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        let lock = self.locked.read().await;
        lock.freshness.blocks_behind()
    }

    pub async fn commit(&self, fee: &GRT) -> Result<ReceiptBorrow, BorrowFail> {
        let mut lock = self.locked.write().await;
        lock.receipts.commit(fee)
    }

    pub async fn expected_performance_utility(&self, u_a: f64) -> SelectionFactor {
        let lock = self.locked.read().await;
        lock.performance.expected_utility(u_a)
    }

    pub async fn expected_reputation_utility(&self) -> Result<SelectionFactor, SelectionError> {
        let lock = self.locked.read().await;
        lock.reputation.expected_utility()
    }

    pub async fn expected_freshness_utility(
        &self,
        freshness_requirements: &BlockRequirements,
        u_a: f64,
        latest_block: u64,
        blocks_behind: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        let lock = self.locked.read().await;
        lock.freshness
            .expected_utility(freshness_requirements, u_a, latest_block, blocks_behind)
    }

    pub async fn has_collateral_for(&self, fee: &GRT) -> bool {
        let lock = self.locked.read().await;
        lock.receipts.has_collateral_for(fee)
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

    pub async fn snapshot(&self, indexing: &Indexing) -> IndexingSnapshot {
        let lock = self.locked.read().await;
        IndexingSnapshot {
            allocation: self.allocation.value_immediate().map(|v| v.to_string()),
            cost_model: self.price_efficiency.model_src.value_immediate(),
            indexing: indexing.clone(),
            performance: lock.performance.clone(),
            freshness: lock.freshness.clone(),
            reputation: lock.reputation.clone(),
        }
    }

    #[cfg(test)]
    pub async fn restore(snapshot: IndexingSnapshot) -> (Indexing, SelectionFactors, IndexingData) {
        let (mut writer, reader) = SelectionFactors::new();
        if let Some(allocation) = snapshot.allocation.and_then(|v| v.parse().ok()) {
            writer.allocation.write(allocation);
        }
        if let Some(model_src) = snapshot.cost_model {
            writer.cost_model.write(model_src);
        }
        {
            let mut lock = reader.locked.write().await;
            lock.performance = snapshot.performance;
            lock.freshness = snapshot.freshness;
            lock.reputation = snapshot.reputation;
        }
        (snapshot.indexing, reader, writer)
    }
}
