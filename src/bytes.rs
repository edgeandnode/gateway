// See https://doc.rust-lang.org/std/macro.concat_bytes.html
#[macro_export]
macro_rules! concat_bytes {
    ($len:expr, [$($slices:expr),* $(,)?]) => {
        {
            let mut cursor = std::io::Cursor::new([0_u8; $len]);
            $(
                std::io::Write::write(&mut cursor, $slices).unwrap();
            )*
            assert!(cursor.position() == $len);
            cursor.into_inner()
        }
    }
}
