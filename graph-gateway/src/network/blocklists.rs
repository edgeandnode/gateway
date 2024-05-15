//! This module contains convenient types to be reused across the network module's different
//! blocklists.

/// The result of a blocklist check.
///
/// This enum is used to represent the result of a blocklist check. It can either be `Allowed` or
/// `Blocked`. It helps to make the code more readable and easier to understand.
#[derive(Clone)]
pub enum BlocklistResult {
    /// The resource is allowed.
    Allowed,
    /// The resource is blocked.
    Blocked,
}

impl BlocklistResult {
    /// Check if the resource is allowed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    /// Check if the resource is blocked.
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}
