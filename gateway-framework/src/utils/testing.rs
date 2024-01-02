use std::{
    io::{Cursor, Write as _},
    sync::Once,
};

use crate::utils::tracing::init_tracing;

pub const BASIC_QUERY: &str = "{ entities { id } }";

pub fn bytes_from_id<const N: usize>(id: usize) -> [u8; N] {
    let mut buf = [0u8; N];
    let mut cursor = Cursor::new(buf.as_mut());
    let _ = cursor.write(&id.to_le_bytes());
    buf
}

pub fn init_test_tracing() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| init_tracing(false))
}
