//! Resolves the IP address of a URL host.
//!
//! This module provides a resolver for URL hosts. The resolver caches the results of host
//! resolution to avoid repeated DNS lookups.
use std::{borrow::Borrow, collections::HashMap, net::IpAddr, time::Duration};

use hickory_resolver::{error::ResolveError, TokioAsyncResolver as DnsResolver};
use parking_lot::RwLock;
use url::{Host, Url};

/// The default timeout for the indexer host resolution.
pub const DEFAULT_INDEXER_HOST_RESOLUTION_TIMEOUT: Duration = Duration::from_millis(1_500);

/// Error that can occur during URL host resolution.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ResolutionError {
    /// The URL is invalid.
    ///
    /// For example, the URL does not contain a host.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    /// Failed to resolve the host.
    ///
    /// This error occurs when the host could not be resolved to an IP address.
    ///
    /// This is a wrapper around [`ResolveError`].
    #[error("dns resolution error: {0}")]
    DnsResolutionError(#[from] ResolveError),

    /// Resolution timed out.
    #[error("timeout")]
    Timeout,
}

impl ResolutionError {
    /// Create a new [`ResolutionError::InvalidUrl`] error from an invalid URL error.
    pub fn invalid_url<E: ToString>(reason: E) -> Self {
        Self::InvalidUrl(reason.to_string())
    }
}

/// A resolver for URL hosts.
///
/// This resolver caches the results of host resolution to avoid repeated DNS lookups.
pub struct HostResolver {
    inner: DnsResolver,
    cache: RwLock<HashMap<String, Result<Vec<IpAddr>, ResolutionError>>>,
    timeout: Duration,
}

impl HostResolver {
    /// Create a new [`HostResolver`].
    ///
    /// If a DNS resolver based on system configuration cannot be created, an error is returned.
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            inner: DnsResolver::tokio_from_system_conf()?,
            cache: Default::default(),
            timeout: DEFAULT_INDEXER_HOST_RESOLUTION_TIMEOUT,
        })
    }

    /// Create a new [`HostResolver`] with a custom timeout.
    ///
    /// If a DNS resolver based on system configuration cannot be created, an error is returned.
    pub fn with_timeout(timeout: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            inner: DnsResolver::tokio_from_system_conf()?,
            cache: Default::default(),
            timeout,
        })
    }

    /// Resolve the IP address of the given domain with a timeout.
    async fn resolve_domain(&self, domain: &str) -> Result<Vec<IpAddr>, ResolutionError> {
        tokio::time::timeout(self.timeout, self.inner.lookup_ip(domain))
            .await
            .map_err(|_| ResolutionError::Timeout)?
            .map_err(Into::into)
            .map(FromIterator::from_iter)
    }

    /// Gets the cached DNS resolution result for the given host.
    ///
    /// This method locks the cache in read mode and returns the cached information.
    fn get_from_cache(&self, host: &str) -> Option<Result<Vec<IpAddr>, ResolutionError>> {
        let cache_read = self.cache.read();
        cache_read.get(host).cloned()
    }

    /// Updates the cache with the given DNS resolution result.
    ///
    /// This method locks the cache in write mode and updates the cache with the given progress
    /// information.
    fn update_cache(&self, host: &str, res: Result<Vec<IpAddr>, ResolutionError>) {
        let mut cache_write = self.cache.write();
        cache_write.insert(host.to_owned(), res);
    }

    /// Resolve the IP address of the given URL.
    ///
    /// The URL is resolved to an IP address. The result is cached so that subsequent calls with the
    /// same URL will return the same result.
    pub async fn resolve_url<U: Borrow<Url>>(
        &self,
        url: U,
    ) -> Result<Vec<IpAddr>, ResolutionError> {
        let url = url.borrow();

        // Check if the result is already cached, otherwise resolve the URLs' associated IP
        // addresses
        let host_str = url
            .host_str()
            .ok_or(ResolutionError::invalid_url("no host"))?;

        match self.get_from_cache(host_str) {
            Some(state) => state,
            None => {
                // Resolve the URL IP addresses
                let host = url.host().ok_or(ResolutionError::invalid_url("no host"))?;

                let resolution = match host {
                    Host::Ipv4(ip) => Ok(vec![IpAddr::V4(ip)]),
                    Host::Ipv6(ip) => Ok(vec![IpAddr::V6(ip)]),
                    Host::Domain(domain) => self.resolve_domain(domain).await,
                };

                // Cache the result
                self.update_cache(host_str, resolution.clone());

                resolution
            }
        }
    }
}
