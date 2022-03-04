use avro_rs::{Schema, Writer};
use bincode;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::redpanda::utils::MessageKind;

lazy_static! {
    pub static ref MESSAGE_SCHEMA: Schema = Schema::parse_str(
        r#"
    {
        "type": "record",
        "name": "ISAScoringError",
        "fields": [
            {"name": "ray_id", "type": "string", "default": "howdy"},
            {"name": "query_id", "type": "long"},
            {"name": "deployment", "type": "string"},
            {"name": "indexer", "type": "string"},
            {"name": "scoring_err", "type": "string"}
        ]
    }
    "#,
    )
    .unwrap();
}
/// Result of a query being failing against a given indexer

#[derive(Serialize, Deserialize)]
pub struct ISAScoringError {
    pub ray_id: String,
    pub query_id: u64,
    pub deployment: String,
    pub indexer: String,
    pub scoring_err: String,
}

impl ISAScoringError {
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

        let bytes = writer.into_inner().expect("Can't conver to bytes");
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
