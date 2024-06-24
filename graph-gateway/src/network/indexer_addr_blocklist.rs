//! Blocklist for indexer addresses.
//!
//! This is an implementation of a static address-based blocklist for indexers.

use std::collections::HashSet;

use gateway_common::blocklist::{Blocklist, Result as BlocklistResult};
use thegraph_core::types::IndexerId;

/// A blocklist for indexer addresses.
#[derive(Debug, Clone, Default)]
pub struct AddrBlocklist {
    blocklist: HashSet<IndexerId>,
}

impl AddrBlocklist {
    /// Create a new [`AddrBlocklist`].
    pub fn new(conf: HashSet<IndexerId>) -> Self {
        Self { blocklist: conf }
    }
}

impl Blocklist for AddrBlocklist {
    type Resource<'a> = &'a IndexerId;

    /// Check if an indexer's address is in the blocklist.
    ///
    /// If the address is in the blocklist, return [`Result::Blocked`], otherwise return
    /// [`Result::Allowed`].
    fn check(&self, addr: &IndexerId) -> BlocklistResult {
        if self.blocklist.contains(addr) {
            BlocklistResult::Blocked
        } else {
            BlocklistResult::Allowed
        }
    }
}
