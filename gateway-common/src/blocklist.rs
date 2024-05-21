//! This module contains convenient types to be reused across different blocklists.

/// The result of a blocklist check.
///
/// This enum is used to represent the result of a blocklist check. It can either be `Allowed` or
/// `Blocked`. It helps to make the code more readable and easier to understand.
#[derive(Clone)]
pub enum Result {
    /// The resource is allowed.
    Allowed,
    /// The resource is blocked.
    Blocked,
}

impl Result {
    /// Check if the resource is allowed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    /// Check if the resource is blocked.
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

/// A blocklist.
///
/// This trait represents a blocklist. It is used to check if a resource is blocked or not.
///
/// The resource is checked against the blocklist and the result is returned as a `BlocklistResult`.
pub trait Blocklist {
    type Resource<'a>: 'a;

    /// Check if a resource is blocked.
    fn check(&self, resource: Self::Resource<'_>) -> Result;
}
