use crate::*;
use std::{
    fs,
    io::{self, Cursor, Write as _},
    sync::Once,
};

pub const BASIC_QUERY: &'static str = "{ entities { id } }";

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

pub fn create_dir(path: &str) {
    match fs::create_dir(path) {
        Ok(()) => (),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
        Err(err) => panic!("{}", err),
    };
}
