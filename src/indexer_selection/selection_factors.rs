use crate::{
    indexer_selection::{
        network_cache::*, performance::*, price_efficiency::*, receipts::*, reputation::*,
        utility::*, BadIndexerReason, Context, Indexing, SecretKey, SelectionError,
    },
    prelude::{shared_lookup, shared_lookup::Reader as _, *},
};
use tokio::{sync::RwLock, time};
use tree_buf::{Decode, Encode};

#[derive(Clone, Debug, Decode, Encode, Eq, PartialEq)]
pub struct IndexingStatus {
    pub block: u64,
}

pub struct SelectionFactors {
    status: Eventual<IndexingStatus>,
    price_efficiency: PriceEfficiency,
    locked: RwLock<LockedState>,
}

#[derive(Default)]
struct LockedState {
    performance: Performance,
    reputation: Reputation,
    freshness: DataFreshness,
    receipts: Receipts,
}

pub struct IndexingData {
    pub cost_model: EventualWriter<CostModelSource>,
    pub status: EventualWriter<IndexingStatus>,
}

#[derive(Debug, Decode, Encode)]
pub struct IndexingSnapshot {
    pub cost_model: Option<CostModelSource>,
    pub status: Option<IndexingStatus>,
    pub indexing: Indexing,
    pub performance: Performance,
    pub freshness: DataFreshness,
    pub reputation: Reputation,
}

impl shared_lookup::Reader for SelectionFactors {
    type Writer = IndexingData;
    fn new() -> (Self::Writer, Self) {
        let (cost_model_writer, cost_model) = Eventual::new();
        let (status_writer, status) = Eventual::new();
        let reader = Self {
            status,
            price_efficiency: PriceEfficiency::new(cost_model),
            locked: RwLock::default(),
        };
        let writer = Self::Writer {
            cost_model: cost_model_writer,
            status: status_writer,
        };
        (writer, reader)
    }
}

impl SelectionFactors {
    pub async fn set_blocks_behind(&self, behind: u64, latest: u64) {
        let mut lock = self.locked.write().await;
        lock.freshness.set_blocks_behind(behind, latest);
    }

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
            cost_model: self.price_efficiency.model_src.value_immediate(),
            status: self.status.value_immediate(),
            indexing: indexing.clone(),
            performance: lock.performance.clone(),
            freshness: lock.freshness.clone(),
            reputation: lock.reputation.clone(),
        }
    }

    pub async fn restore(snapshot: IndexingSnapshot) -> (Indexing, SelectionFactors, IndexingData) {
        let (mut writer, reader) = SelectionFactors::new();
        if let Some(model_src) = snapshot.cost_model {
            writer.cost_model.write(model_src);
        }
        if let Some(status) = snapshot.status {
            writer.status.write(status);
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
