use crate::{
    indexer_selection::{
        selection_factors::IndexingDataSnapshot, IndexerDataReader, IndexerDataWriter,
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
