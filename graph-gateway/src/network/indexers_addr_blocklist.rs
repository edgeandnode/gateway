//! Blocklist for indexer addresses.
//!
//! This is an implementation of a static address-based blocklist for indexers.

use std::collections::HashSet;

use alloy_primitives::Address;

use super::blocklists::BlockState;

/// A blocklist for indexer addresses.
#[derive(Debug, Clone, Default)]
pub struct AddrBlocklist {
    blocklist: HashSet<Address>,
}

impl AddrBlocklist {
    /// Create a new [`AddrBlocklist`].
    pub fn new(conf: HashSet<Address>) -> Self {
        Self { blocklist: conf }
    }

    /// Check if an indexer's address is in the blocklist.
    ///
    /// If the address is in the blocklist, return [`BlockState::Blocked`], otherwise return
    /// [`BlockState::Allowed`].
    pub fn check(&self, addr: &Address) -> BlockState {
        if self.blocklist.contains(addr) {
            BlockState::Blocked
        } else {
            BlockState::Allowed
        }
    }
}
