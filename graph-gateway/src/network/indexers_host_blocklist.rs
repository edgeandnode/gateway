//! This module contains the [`IndexerHost`] struct, which is used to block indexers based
//! on the host's IP address.
//!
//! The blocklist is loaded from a CSV file containing rows of `IpNetwork,Country`. Indexer URLs
//! are resolved to IP addresses using a DNS resolver, and then checked against the blocklist. The
//! result is cached so that subsequent calls with the same URL will return the same result.

use std::{collections::HashSet, fs, net::IpAddr, path::Path};

use anyhow::Context as _;
use ipnetwork::IpNetwork;

use super::blocklists::BlockState;

/// Load the IP blocklist from a CSV file.
///
/// The CSV file should contain rows of `IpNetwork,Country`.
pub fn load_ip_blocklist_conf(db_path: &Path) -> anyhow::Result<HashSet<IpNetwork>> {
    let db = fs::read_to_string(db_path).context("IP blocklist DB")?;
    Ok(db
        .lines()
        .filter_map(|line| line.split_once(',')?.0.parse().ok())
        .collect())
}

/// An IP blocklist for indexers.
///
/// This blocklist is used to block certain indexers based on the host IP address. The blocklist is
/// loaded from a CSV file containing rows of `IpNetwork,Country`.
///
/// Indexer URLs are resolved to IP addresses, which are then checked against the blocklist. The
/// result is cached so that subsequent calls with the same URL will return the same result.
#[derive(Debug, Clone, Default)]
pub struct HostBlocklist {
    conf: HashSet<IpNetwork>,
}

impl HostBlocklist {
    /// Create a new [`HostBlocklist`] from the given blocklist.
    pub fn new(conf: HashSet<IpNetwork>) -> Self {
        tracing::debug!(blocked_networks = conf.len());
        Self { conf }
    }

    /// Check if any of the resolved IP addresses are blocked.
    ///
    /// If any of the resolved IP addresses are blocked, the function will return
    /// [`BlockState::Blocked`], otherwise it will return [`BlockState::Allowed`].
    pub fn check(&self, addrs: &[IpAddr]) -> BlockState {
        // Check if any of the IP addresses are contained in any of the blocked networks
        if addrs
            .iter()
            .any(|addr| self.conf.iter().any(|net| net.contains(*addr)))
        {
            BlockState::Blocked
        } else {
            BlockState::Allowed
        }
    }
}
