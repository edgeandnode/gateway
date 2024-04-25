//! Resolves the IP address of a URL host.
//!
//! This module provides a resolver for URL hosts. The resolver caches the results of host
//! resolution to avoid repeated DNS lookupsblock.
use std::{borrow::Borrow, collections::HashMap, net::IpAddr};

use hickory_resolver::{error::ResolveError, TokioAsyncResolver as DnsResolver};
use url::{Host, Url};

/// Error that can occur during URL host resolution.
#[derive(Debug, Clone, thiserror::Error)]
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
    #[error("failed to resolve host: {0}")]
    DnsResolveError(#[from] ResolveError),
}

impl ResolutionError {
    /// Create a new [`ResolutionError::InvalidUrl`] error from an invalid URL error.
    pub fn invalid_url<E: ToString>(reason: E) -> Self {
        Self::InvalidUrl(reason.to_string())
    }

    /// Create a new [`ResolutionError::DnsResolveError`] from a [`ResolveError`].
    pub fn resolve_error(error: ResolveError) -> Self {
        Self::DnsResolveError(error)
    }
}

/// A resolver for URL hosts.
///
/// This resolver caches the results of host resolution to avoid repeated DNS lookups.
pub struct HostResolver {
    inner: DnsResolver,
    cache: HashMap<String, Result<Vec<IpAddr>, ResolutionError>>,
}

impl HostResolver {
    /// Create a new [`HostResolver`].
    ///
    /// If a DNS resolver based on system configuration cannot be created, an error is returned.
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            inner: DnsResolver::tokio_from_system_conf()?,
            cache: Default::default(),
        })
    }

    /// Resolve the IP address of the given domain.
    async fn resolve_domain(&mut self, domain: &str) -> Result<Vec<IpAddr>, ResolveError> {
        self.inner
            .lookup_ip(domain)
            .await
            .map(|lookup| lookup.into_iter().collect())
    }

    /// Resolve the IP address of the given URL.
    ///
    /// The URL is resolved to an IP address. The result is cached so that subsequent calls with the
    /// same URL will return the same result.
    pub async fn resolve_url<U: Borrow<Url>>(
        &mut self,
        url: U,
    ) -> Result<Vec<IpAddr>, ResolutionError> {
        let url = url.borrow();

        // Check if the result is already cached, otherwise resolve the URLs' associated IP
        // addresses
        let host_str = url
            .host_str()
            .ok_or(ResolutionError::invalid_url("no host"))?;
        let resolution = match self.cache.get(host_str).cloned() {
            Some(state) => state,
            None => {
                // Resolve the URL IP addresses
                let host = url.host().ok_or(ResolutionError::invalid_url("no host"))?;

                let resolution = match host {
                    Host::Ipv4(ip) => Ok(vec![IpAddr::V4(ip)]),
                    Host::Ipv6(ip) => Ok(vec![IpAddr::V6(ip)]),
                    Host::Domain(domain) => Ok(self.resolve_domain(domain).await?),
                };

                // Cache the result
                self.cache.insert(host_str.to_string(), resolution.clone());

                resolution
            }
        };

        resolution
    }
}
