use avro_rs::{to_value, Schema, Writer};
use bincode;
use lazy_static::lazy_static;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::prelude::UDecimal;
use crate::redpanda::messages::base_writer::MessageWriter;
use crate::redpanda::utils::MessageKind;

use serde_bytes;
lazy_static! {
    pub static ref MESSAGE_SCHEMA: Schema = Schema::parse_str(
        r#"
    {
        "type": "record",
        "name": "ISAScoringSample",
        "fields": [
            {"name": "ray_id", "type": "string", "default": "howdy"},
            {"name": "query_id", "type": "string"},
            {"name": "deployment", "type": "bytes"},
            {"name": "address", "type": "bytes"},
            {"name": "fee", "type": "double"},
            {"name": "slashable", "type": "double"},
            {"name": "utility", "type": "double"},
            {"name": "economic_security", "type": "double"},
            {"name": "price_efficiency", "type": "double"},
            {"name": "data_freshness", "type": "double"},
            {"name": "performance", "type": "double"},
            {"name": "reputation", "type": "double"},
            {"name": "sybil", "type": "double"},
            {"name": "blocks_behind", "type": "long"},
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
    pub query_id: String,
    #[serde(with = "serde_bytes")]
    pub deployment: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub address: Vec<u8>,
    pub fee: f64,
    pub slashable: f64,
    pub utility: f64,
    pub economic_security: f64,
    pub price_efficiency: f64,
    pub data_freshness: f64,
    pub performance: f64,
    pub reputation: f64,
    pub sybil: f64,
    pub blocks_behind: u64,
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

impl Default for ISAScoringSample {
    fn default() -> ISAScoringSample {
        let deployment_vec = random_bytes(20);
        let address_vec = random_bytes(30);

        ISAScoringSample {
            ray_id: String::from("null_ray"),
            query_id: String::from("null_query"),
            deployment: deployment_vec,
            address: address_vec,
            fee: Default::default(),
            slashable: Default::default(),
            utility: Default::default(),
            economic_security: Default::default(),
            price_efficiency: Default::default(),
            data_freshness: Default::default(),
            performance: Default::default(),
            reputation: Default::default(),
            sybil: Default::default(),
            blocks_behind: Default::default(),
            message: Default::default(),
        }
    }
}
