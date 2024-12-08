use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    time::Duration,
};

use hickory_resolver::TokioAsyncResolver as DnsResolver;
use ipnetwork::IpNetwork;
use parking_lot::RwLock;
use url::{Host, Url};

use crate::errors::UnavailableReason;

pub struct HostFilter {
    blocklist: HashSet<IpNetwork>,
    resolver: DnsResolver,
    cache: RwLock<HashMap<String, Vec<IpAddr>>>,
}

impl HostFilter {
    pub fn new(blocklist: HashSet<IpNetwork>) -> anyhow::Result<Self> {
        Ok(Self {
            blocklist,
            resolver: DnsResolver::tokio_from_system_conf()?,
            cache: Default::default(),
        })
    }

    pub async fn check(&self, url: &Url) -> Result<(), UnavailableReason> {
        if self.blocklist.is_empty() {
            return Ok(());
        }

        let host_str = url.host_str().ok_or_else(UnavailableReason::invalid_url)?;
        let cached_lookup = {
            let cache = self.cache.read();
            cache.get(host_str).cloned()
        };
        let lookup = match cached_lookup {
            Some(lookup) => lookup,
            None => {
                let host = url.host().ok_or_else(UnavailableReason::invalid_url)?;
                let lookup = match host {
                    Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
                    Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
                    Host::Domain(host) => self.resolve_host(host).await.map_err(|_| {
                        UnavailableReason::NoStatus("unable to resolve host".to_string())
                    })?,
                };
                self.cache
                    .write()
                    .insert(host_str.to_string(), lookup.clone());
                lookup
            }
        };

        if lookup
            .into_iter()
            .any(|ip| self.blocklist.iter().any(|net| net.contains(ip)))
        {
            return Err(UnavailableReason::Blocked("bad host".to_string()));
        }

        Ok(())
    }

    async fn resolve_host(&self, host: &str) -> anyhow::Result<Vec<IpAddr>> {
        let lookup =
            tokio::time::timeout(Duration::from_secs(5), self.resolver.lookup_ip(host)).await??;
        Ok(lookup.into_iter().collect())
    }
}
