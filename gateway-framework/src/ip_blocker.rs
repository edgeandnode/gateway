use std::{
    collections::{HashMap, HashSet},
    fs,
    net::IpAddr,
    path::Path,
};

use anyhow::Context as _;
use ipnetwork::IpNetwork;
use url::{Host, Url};

pub struct IpBlocker {
    blocked_networks: HashSet<IpNetwork>,
    dns_resolver: hickory_resolver::TokioAsyncResolver,
    cache: HashMap<String, Result<(), String>>,
}

impl IpBlocker {
    pub fn new(db_path: Option<&Path>) -> anyhow::Result<Self> {
        let db = match db_path {
            Some(path) => fs::read_to_string(path).context("IP blocker DB")?,
            None => "".into(),
        };
        let blocked_networks: HashSet<IpNetwork> = db
            .lines()
            .filter_map(|line| line.split_once(',')?.0.parse().ok())
            .collect();
        tracing::debug!(blocked_networks = blocked_networks.len());
        Ok(Self {
            blocked_networks,
            dns_resolver: hickory_resolver::TokioAsyncResolver::tokio_from_system_conf()?,
            cache: Default::default(),
        })
    }

    pub async fn is_ip_blocked(&mut self, url: &Url) -> Result<(), String> {
        let host_str = url.host_str().ok_or_else(|| "missing host".to_string())?;
        if let Some(decision) = self.cache.get(host_str) {
            return decision.clone();
        }

        let host = url.host().ok_or_else(|| "missing host".to_string())?;
        let addrs = match host {
            Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
            Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
            Host::Domain(host) => self
                .dns_resolver
                .lookup_ip(host)
                .await
                .into_iter()
                .flat_map(|lookup| lookup.into_iter())
                .collect(),
        };
        let blocked = addrs
            .into_iter()
            .any(|addr| self.blocked_networks.iter().any(|net| net.contains(addr)));
        let result = blocked.then(|| Err("blocked".into())).unwrap_or(Ok(()));
        self.cache.insert(host_str.to_string(), result.clone());
        result
    }
}
