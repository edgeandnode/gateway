use crate::{
    indexer_selection::{
        DataFreshness, IndexerUtilityTracker, Indexing, IndexingData, Performance, PriceEfficiency,
        Receipts, Reputation,
    },
    prelude::*,
};
use tree_buf::{Decode, Encode};

#[derive(Debug, Default, Decode, Encode)]
pub struct Snapshot {
    pub slashing_percentage: Bytes32,
    pub usd_to_grt_conversion: Bytes32,
    pub indexings: Vec<(Indexing, IndexingDataSnapshot)>,
    pub indexers: Vec<(Address, IndexerUtilityTrackerSnapshot)>,
}

#[derive(Debug, Decode, Encode)]
pub struct IndexingDataSnapshot {
    pub performance: Performance,
    pub freshness: DataFreshness,
    pub price_efficiency: Option<(String, String)>,
    pub reputation: Reputation,
}

#[derive(Debug, Decode, Encode)]
pub struct IndexerUtilityTrackerSnapshot {
    stake: Option<Bytes32>,
    delegated_stake: Option<Bytes32>,
}

impl IndexingDataSnapshot {
    pub fn snapshot(from: &IndexingData) -> Self {
        Self {
            performance: from.performance.clone(),
            freshness: from.freshness.clone(),
            price_efficiency: from.price_efficiency.model_source.clone(),
            reputation: from.reputation.clone(),
        }
    }

    pub fn restore(from: Self) -> IndexingData {
        let mut price_efficiency = PriceEfficiency::default();
        price_efficiency.model_source = from.price_efficiency;
        IndexingData {
            performance: from.performance,
            freshness: from.freshness,
            price_efficiency,
            reputation: from.reputation,
            receipts: Receipts::default(),
        }
    }
}

impl IndexerUtilityTrackerSnapshot {
    pub fn snapshot(from: &IndexerUtilityTracker) -> Self {
        Self {
            stake: from.stake.map(|v| v.to_little_endian().into()),
            delegated_stake: from.delegated_stake.map(|v| v.to_little_endian().into()),
        }
    }

    pub fn restore(from: Self) -> IndexerUtilityTracker {
        IndexerUtilityTracker {
            stake: from.stake.map(|v| GRT::from_little_endian(&v)),
            delegated_stake: from.delegated_stake.map(|v| GRT::from_little_endian(&v)),
        }
    }
}
