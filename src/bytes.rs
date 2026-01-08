//! Byte Manipulation Utilities
//!
//! Provides the [`concat_bytes!`] macro for concatenating byte slices at compile time.
//!
//! # Example
//!
//! ```ignore
//! let signature = concat_bytes!(65, [&[v], &r[..], &s[..]]);
//! ```

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
