use crate::{
    indexer_selection::{
        DataFreshness, IndexerDataReader, IndexerDataWriter, Indexing, IndexingData, Performance,
        PriceEfficiency, Receipts, Reputation,
    },
    prelude::{shared_lookup::Reader as _, *},
};
use tree_buf::{Decode, Encode};

#[derive(Debug, Default, Decode, Encode)]
pub struct Snapshot {
    pub slashing_percentage: Bytes32,
    pub usd_to_grt_conversion: Bytes32,
    pub indexers: Vec<IndexerDataSnapshot>,
    pub indexings: Vec<IndexingDataSnapshot>,
}

#[derive(Debug, Decode, Encode)]
pub struct IndexerDataSnapshot {
    address: Address,
    stake: Option<Bytes32>,
    delegated_stake: Option<Bytes32>,
}

impl From<(&Address, &IndexerDataReader)> for IndexerDataSnapshot {
    fn from(from: (&Address, &IndexerDataReader)) -> Self {
        Self {
            address: from.0.clone(),
            stake: from
                .1
                .stake
                .value_immediate()
                .map(|s| s.to_little_endian().into()),
            delegated_stake: from
                .1
                .delegated_stake
                .value_immediate()
                .map(|s| s.to_little_endian().into()),
        }
    }
}

impl Into<(Address, IndexerDataReader, IndexerDataWriter)> for IndexerDataSnapshot {
    fn into(self) -> (Address, IndexerDataReader, IndexerDataWriter) {
        let (mut writer, reader) = IndexerDataReader::new();
        if let Some(stake) = self.stake {
            writer.stake.write(GRT::from_little_endian(&stake));
        }
        if let Some(delegated_stake) = self.delegated_stake {
            writer
                .delegated_stake
                .write(GRT::from_little_endian(&delegated_stake));
        }
        (self.address, reader, writer)
    }
}

#[derive(Debug, Decode, Encode)]
pub struct IndexingDataSnapshot {
    pub indexing: Indexing,
    pub performance: Performance,
    pub freshness: DataFreshness,
    pub price_efficiency: Option<(String, String)>,
    pub reputation: Reputation,
}

impl From<(&Indexing, &IndexingData)> for IndexingDataSnapshot {
    fn from(from: (&Indexing, &IndexingData)) -> Self {
        Self {
            indexing: from.0.clone(),
            performance: from.1.performance.clone(),
            freshness: from.1.freshness.clone(),
            price_efficiency: from.1.price_efficiency.model_source.clone(),
            reputation: from.1.reputation.clone(),
        }
    }
}

impl Into<(Indexing, IndexingData)> for IndexingDataSnapshot {
    fn into(self) -> (Indexing, IndexingData) {
        let mut price_efficiency = PriceEfficiency::default();
        price_efficiency.model_source = self.price_efficiency;
        let data = IndexingData {
            performance: self.performance,
            freshness: self.freshness,
            price_efficiency,
            reputation: self.reputation,
            receipts: Receipts::default(),
        };
        (self.indexing, data)
    }
}
