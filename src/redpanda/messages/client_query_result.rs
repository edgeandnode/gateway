use avro_rs::{Schema, Writer};
use bincode;
use lazy_static::lazy_static;

use crate::redpanda::utils::MessageKind;
use serde::{Deserialize, Serialize};

lazy_static! {
    pub static ref MESSAGE_SCHEMA: Schema = Schema::parse_str(
        r#"
    {
        "type": "record",
        "name": "ClientQueryResult",
        "fields": [
            {"name": "ray_id", "type": "string", "default": "howdy"},
            {"name": "query_id", "type": "long"},
            {"name": "deployment", "type": "string"},
            {"name": "network", "type": "string"},
            {"name": "api_key", "type": "string"},
            {"name": "query", "type": "string"},
            {"name": "response_time", "type": "int"},
            {"name": "variables", "type": "string"},
            {"name": "status", "type": "string"}
        ]
    }
    "#,
    )
    .unwrap();
}
/// Statistics on the result of a query and it's status

#[derive(Serialize, Deserialize)]
pub struct ClientQueryResult {
    pub ray_id: String,
    pub query_id: u64,
    pub deployment: String,
    pub network: String,
    pub api_key: String,
    pub query: String,
    pub response_time: u32,
    pub variables: String,
    pub status: String,
}

impl ClientQueryResult {
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
