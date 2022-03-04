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
        "name": "ISAScoringSample",
        "fields": [
            {"name": "ray_id", "type": "string", "default": "howdy"},
            {"name": "query_id", "type": "long"},
            {"name": "deployment", "type": "string"},
            {"name": "address", "type": "string"},
            {"name": "fee", "type": "string"},
            {"name": "slashable", "type": "string"},
            {"name": "utility", "type": "double"},
            {"name": "economic_security", "type": "double"},
            {"name": "price_efficiency", "type": "double"},
            {"name": "data_freshness", "type": "double"},
            {"name": "performance", "type": "double"},
            {"name": "reputation", "type": "double"},
            {"name": "sybil", "type": "double"},
            {"name": "blocks_behind", "type": "long"},
            {"name": "url", "type": "string"},
            {"name": "message", "type": "string"}
        ]
    }
    "#,
    )
    .unwrap();
}
/// Result of a query being scored against an indexer

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ISAScoringSample {
    pub ray_id: String,
    pub query_id: u64,
    pub deployment: String,
    pub address: String,
    pub fee: String,
    pub slashable: String,
    pub utility: f64,
    pub economic_security: f64,
    pub price_efficiency: f64,
    pub data_freshness: f64,
    pub performance: f64,
    pub reputation: f64,
    pub sybil: f64,
    pub blocks_behind: u64,
    pub url: String,
    pub message: String,
}

impl ISAScoringSample {
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
