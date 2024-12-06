use std::{collections::HashMap, net::IpAddr, time::Duration};

use hickory_resolver::TokioAsyncResolver as DnsResolver;
use parking_lot::RwLock;
use url::{Host, Url};

use crate::errors::UnavailableReason;

pub struct HostResolver {
    inner: DnsResolver,
    cache: RwLock<HashMap<String, Result<Vec<IpAddr>, UnavailableReason>>>,
    timeout: Duration,
}

impl HostResolver {
    pub fn new(timeout: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            inner: DnsResolver::tokio_from_system_conf()?,
            cache: Default::default(),
            timeout,
        })
    }

    async fn resolve_domain(&self, domain: &str) -> anyhow::Result<Vec<IpAddr>> {
        let lookup = tokio::time::timeout(self.timeout, self.inner.lookup_ip(domain)).await??;
        Ok(lookup.into_iter().collect())
    }

    pub async fn resolve_url(&self, url: &Url) -> Result<Vec<IpAddr>, UnavailableReason> {
        let host_str = url.host_str().ok_or_else(UnavailableReason::invalid_url)?;
        let cached_response = {
            let cache = self.cache.read();
            cache.get(host_str).cloned()
        };
        match cached_response {
            Some(result) => result,
            None => {
                let host = url.host().ok_or_else(UnavailableReason::invalid_url)?;

                let result = match host {
                    Host::Ipv4(ip) => Ok(vec![IpAddr::V4(ip)]),
                    Host::Ipv6(ip) => Ok(vec![IpAddr::V6(ip)]),
                    Host::Domain(domain) => self.resolve_domain(domain).await.map_err(|err| {
                        UnavailableReason::NoStatus(format!("DNS resolution error: {err}"))
                    }),
                };

                self.cache
                    .write()
                    .insert(host_str.to_string(), result.clone());

                result
            }
        }
    }
}
