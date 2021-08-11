use crate::prelude::*;
use std::io::{Cursor, Write as _};

pub const BASIC_QUERY: &'static str = "{ entities { id } }";

pub fn bytes_from_id<const N: usize>(id: usize) -> [u8; N] {
    let mut buf = [0u8; N];
    let mut cursor = Cursor::new(buf.as_mut());
    let _ = cursor.write(&id.to_le_bytes());
    buf
}

pub fn default_cost_model(price: GRT) -> String {
    format!("default => {};", price)
}
