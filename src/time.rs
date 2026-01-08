//! Time Utilities
//!
//! Simple timestamp helpers for consistent time handling.

use std::time::SystemTime;

/// Return milliseconds since Unix epoch
pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
