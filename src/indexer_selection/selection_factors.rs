use crate::{
    indexer_selection::{
        network_cache::*, performance::*, price_efficiency::*, receipts::*, reputation::*,
        utility::*, BadIndexerReason, Context, Indexing, SecretKey, SelectionError,
    },
    prelude::{shared_lookup, shared_lookup::Reader as _, *},
};
use tokio::time;
use tree_buf::{Decode, Encode};

#[derive(Clone, Debug, Decode, Encode, Eq, PartialEq)]
pub struct IndexingStatus {
    pub block: u64,
}

pub struct SelectionFactors {
    status: Eventual<IndexingStatus>,
    price_efficiency: PriceEfficiency,
    performance: Performance,
    reputation: Reputation,
    freshness: DataFreshness,
    receipts: Receipts,
}

pub struct IndexingData {
    pub cost_model: EventualWriter<CostModelSource>,
    pub status: EventualWriter<IndexingStatus>,
}

impl shared_lookup::Reader for SelectionFactors {
    type Writer = IndexingData;
    fn new() -> (Self::Writer, Self) {
        let (cost_model_writer, cost_model) = Eventual::new();
        let (status_writer, status) = Eventual::new();
        let reader = Self {
            status,
            price_efficiency: PriceEfficiency::new(cost_model),
            // TODO: stop using default?
            performance: Performance::default(),
            reputation: Reputation::default(),
            freshness: DataFreshness::default(),
            receipts: Receipts::default(),
        };
        let writer = Self::Writer {
            cost_model: cost_model_writer,
            status: status_writer,
        };
        (writer, reader)
    }
}

impl SelectionFactors {
    #[inline]
    pub fn set_blocks_behind(&mut self, behind: u64, latest: u64) {
        self.freshness.set_blocks_behind(behind, latest);
    }

    #[inline]
    pub fn add_transfer(&mut self, transfer_id: Bytes32, collateral: &GRT, secret: SecretKey) {
        self.receipts.add_transfer(transfer_id, collateral, secret);
    }

    pub fn observe_successful_query(&mut self, duration: time::Duration, receipt: &[u8]) {
        self.receipts.release(receipt, QueryStatus::Success);
        self.performance.add_successful_query(duration);
        self.reputation.add_successful_query();
    }

    pub fn observe_failed_query(&mut self, receipt: &[u8], status: QueryStatus) {
        self.receipts.release(receipt, status);
        self.reputation.add_failed_query();
    }

    pub fn decay(&mut self, retain: f64) {
        self.performance.decay(retain);
        self.reputation.decay(retain);
    }

    #[inline]
    pub fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        self.freshness.blocks_behind()
    }

    #[inline]
    pub fn commit(&mut self, fee: &GRT) -> Result<ReceiptBorrow, BorrowFail> {
        self.receipts.commit(fee)
    }

    #[inline]
    pub fn expected_performance_utility(&self, u_a: f64) -> SelectionFactor {
        self.performance.expected_utility(u_a)
    }

    #[inline]
    pub fn expected_reputation_utility(&self) -> Result<SelectionFactor, SelectionError> {
        self.reputation.expected_utility()
    }

    #[inline]
    pub fn expected_freshness_utility(
        &self,
        freshness_requirements: &BlockRequirements,
        u_a: f64,
        latest_block: u64,
        blocks_behind: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        self.freshness
            .expected_utility(freshness_requirements, u_a, latest_block, blocks_behind)
    }

    #[inline]
    pub fn has_collateral_for(&self, fee: &GRT) -> bool {
        self.receipts.has_collateral_for(fee)
    }

    #[inline]
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

#[derive(Debug, Decode, Encode)]
pub struct IndexingDataSnapshot {
    pub cost_model: Option<CostModelSource>,
    pub status: Option<IndexingStatus>,
    pub indexing: Indexing,
    pub performance: Performance,
    pub freshness: DataFreshness,
    pub reputation: Reputation,
}

impl From<(&Indexing, &SelectionFactors)> for IndexingDataSnapshot {
    fn from(from: (&Indexing, &SelectionFactors)) -> Self {
        Self {
            cost_model: from.1.price_efficiency.model_src.value_immediate(),
            status: from.1.status.value_immediate(),
            indexing: from.0.clone(),
            performance: from.1.performance.clone(),
            freshness: from.1.freshness.clone(),
            reputation: from.1.reputation.clone(),
        }
    }
}

impl Into<(Indexing, SelectionFactors, IndexingData)> for IndexingDataSnapshot {
    fn into(self) -> (Indexing, SelectionFactors, IndexingData) {
        let (mut writer, mut reader) = SelectionFactors::new();
        if let Some(model_src) = self.cost_model {
            writer.cost_model.write(model_src);
        }
        if let Some(status) = self.status {
            writer.status.write(status);
        }
        reader.performance = self.performance;
        reader.freshness = self.freshness;
        reader.reputation = self.reputation;
        (self.indexing, reader, writer)
    }
}
