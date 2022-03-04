use crate::redpanda::utils::MessageKind;
use avro_rs::{Schema, Writer};
use bincode;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

lazy_static! {
    pub static ref MESSAGE_SCHEMA: Schema = Schema::parse_str(
        r#"
    {
        "type": "record",
        "name": "IndexerAttempt",
        "fields": [
            {"name": "ray_id", "type": "string", "default": "howdy"},
            {"name": "query_id", "type": "long"},
            {"name": "attempt_index", "type": "int"},
            {"name": "indexer", "type": "string"},
            {"name": "allocation", "type": "string"},
            {"name": "fee", "type": "string"},
            {"name": "utility", "type": "double"},
            {"name": "blocks_behind", "type": "long"},
            {"name": "response_time_ms", "type": "int"},
            {"name": "status", "type": "string"},
            {"name": "status_code", "type": "int"}

        ]
    }
    "#,
    )
    .unwrap();
}
/// Result of a query being attempted on an indexer

#[derive(Serialize, Deserialize)]
pub struct IndexerAttempt {
    pub ray_id: String,
    pub query_id: u64,
    pub attempt_index: usize,
    pub indexer: String,
    pub allocation: String,
    pub fee: String,
    pub utility: f64,
    pub blocks_behind: u64,
    pub response_time_ms: u32,
    pub status: String,
    pub status_code: u32,
}

impl IndexerAttempt {
    fn get_schema(&self) -> &Schema {
        &MESSAGE_SCHEMA
    }

    fn write_avro(&self) -> Vec<u8> {
        let mut writer = Writer::new(self.get_schema(), Vec::new());

        let res = writer.append_ser(self);

        match res {
            Ok(_res) => (),
            Err(err) => {
                panic!("{:?}", err);
            }
        };

        let bytes = writer.into_inner().expect("Can't convert to bytes");
        bytes
    }

    fn write_default(&self) -> Vec<u8> {
        let encoded = bincode::serialize(self).unwrap();
        encoded
    }

    pub fn write(&self, method: MessageKind) -> Vec<u8> {
        match method {
            MessageKind::JSON => serde_json::to_vec(self).unwrap(),
            MessageKind::AVRO => self.write_avro(),
            _ => self.write_default(),
        }
    }
}
