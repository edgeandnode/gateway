use crate::redpanda::utils::MessageKind;
use avro_rs::Schema;
use avro_rs::Writer;
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
            {"name": "rejection", "type": "string"}

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
    pub rejection: String,
}

impl IndexerAttempt {
    fn get_schema(&self) -> &Schema {
        &MESSAGE_SCHEMA
    }

    fn write_avro(&self) -> Vec<u8> {
        let mut writer = Writer::new(self.get_schema(), Vec::new());

        let res = writer.append_ser(self);

        match res {
            Ok(res) => (),
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

fn random_bytes(size: u32) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    return random_bytes;
}

impl Default for IndexerAttempt {
    fn default() -> IndexerAttempt {
        IndexerAttempt {
            ray_id: String::from("null_ray"),
            query_id: Default::default(),
            attempt_index: Default::default(),
            indexer: Default::default(),
            allocation: Default::default(),
            fee: Default::default(),
            utility: Default::default(),
            blocks_behind: Default::default(),
            response_time_ms: Default::default(),
            status: Default::default(),
            rejection: Default::default(),
        }
    }
}
