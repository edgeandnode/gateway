use crate::prelude::*;
use tree_buf::{Decode, Encode};

pub struct IndexerDataReader {
    pub stake: Eventual<GRT>,
    pub delegated_stake: Eventual<GRT>,
}

pub struct IndexerDataWriter {
    pub stake: EventualWriter<GRT>,
    pub delegated_stake: EventualWriter<GRT>,
}

#[derive(Debug, Decode, Encode)]
pub struct IndexerSnapshot {
    address: Address,
    stake: Option<Bytes32>,
    delegated_stake: Option<Bytes32>,
}

impl Reader for IndexerDataReader {
    type Writer = IndexerDataWriter;
    fn new() -> (Self::Writer, Self) {
        let (stake_writer, stake) = Eventual::new();
        let (delegated_stake_writer, delegated_stake) = Eventual::new();
        (
            IndexerDataWriter {
                stake: stake_writer,
                delegated_stake: delegated_stake_writer,
            },
            IndexerDataReader {
                stake,
                delegated_stake,
            },
        )
    }
}

impl From<(&Address, &IndexerDataReader)> for IndexerSnapshot {
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

impl Into<(Address, IndexerDataReader, IndexerDataWriter)> for IndexerSnapshot {
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
