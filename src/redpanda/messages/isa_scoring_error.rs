use avro_rs::Schema;
use avro_rs::Writer;
use bincode;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::redpanda::utils::MessageKind;
use serde_bytes;

lazy_static! {
    pub static ref MESSAGE_SCHEMA: Schema = Schema::parse_str(
        r#"
    {
        "type": "record",
        "name": "ISAScoringError",
        "fields": [
            {"name": "ray_id", "type": "string", "default": "howdy"},
            {"name": "query_id", "type": "long"},
            {"name": "deployment", "type": "bytes"},
            {"name": "indexer", "type": "bytes"},
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
    #[serde(with = "serde_bytes")]
    pub deployment: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub indexer: Vec<u8>,
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
            Ok(res) => (),
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

fn random_bytes(size: u32) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    return random_bytes;
}

impl Default for ISAScoringError {
    fn default() -> ISAScoringError {
        let deployment_vec = random_bytes(20);
        let indexer_vec = random_bytes(30);

        ISAScoringError {
            ray_id: String::from("null_ray"),
            query_id: Default::default(),
            deployment: deployment_vec,
            indexer: indexer_vec,
            scoring_err: Default::default(),
        }
    }
}
