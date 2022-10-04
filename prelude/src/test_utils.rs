use crate::*;
use anyhow::Result;
use std::{
    fs::{self, File},
    io::{self, Cursor, Write as _},
    path::PathBuf,
    sync::Once,
};

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

pub fn create_test_output(path: &str) -> Result<File> {
    let dir = PathBuf::try_from("test-outputs").unwrap();
    match fs::create_dir(&dir) {
        Ok(()) => (),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
        Err(err) => anyhow::bail!(err),
    };
    let path = dir.join(path);
    Ok(File::create(path)?)
}
