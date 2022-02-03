use crate::redpanda::utils::MessageKind;
use avro_rs::{Schema, Writer};
use serde_json;
pub trait MessageWriter: serde::Serialize {
    fn write(&self, method: MessageKind) -> Vec<u8> {
        match method {
            MessageKind::JSON => serde_json::to_vec(self).unwrap(),
            MessageKind::AVRO => self.write_avro(),
            _ => self.write_default(),
        }
    }

    fn get_schema(&self) -> &Schema;

    fn write_default(&self) -> Vec<u8> {
        let encoded = bincode::serialize(self).unwrap();
        encoded
    }

    fn write_avro(&self) -> Vec<u8> {
        let mut writer = Writer::new(self.get_schema(), Vec::new());
        writer.append_ser(self).unwrap();
        let bytes = writer.into_inner().unwrap();
        bytes
    }
}
