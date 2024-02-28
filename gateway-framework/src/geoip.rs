use maxminddb::{geoip2, Reader};
use std::{
    collections::{BTreeSet, HashMap},
    net::IpAddr,
    path::Path,
};
use url::{Host, Url};

pub struct GeoIp {
    reader: Reader<Vec<u8>>,
    blocked_countries: BTreeSet<String>,
    dns_resolver: hickory_resolver::TokioAsyncResolver,
    cache: HashMap<String, Result<(), String>>,
}

impl GeoIp {
    pub fn new(db_file: impl AsRef<Path>, blocked_countries: Vec<String>) -> anyhow::Result<Self> {
        Ok(Self {
            reader: Reader::open_readfile(db_file)?,
            blocked_countries: blocked_countries.into_iter().collect(),
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
                .map_err(|err| err.to_string())?
                .into_iter()
                .collect(),
        };
        let blocked = addrs
            .into_iter()
            .filter_map(|addr| {
                self.reader
                    .lookup::<geoip2::Country>(addr)
                    .ok()?
                    .country?
                    .iso_code
            })
            .any(|country| self.blocked_countries.contains(country));
        let result = blocked.then_some(()).ok_or_else(|| "blocked".to_string());
        self.cache.insert(host_str.to_string(), result.clone());
        result
    }
}
