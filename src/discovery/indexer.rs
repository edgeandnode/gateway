use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, ensure};
use ipnetwork::IpNetwork;
use semver::Version;
use serde::Deserialize;
use thegraph_core::types::IndexerId;
use thegraph_graphql_http::http_client::ReqwestExt;
use url::Url;

use super::{network_subgraph, Indexer};
use crate::errors::UnavailableReason;

pub struct Resolver {
    http: reqwest::Client,
    blocklist: HashSet<IndexerId>,
    ip_blocker: IpBlocker,
    min_versions: Versions,
}

pub struct Versions {
    pub indexer_service: Version,
    pub graph_node: Version,
}

impl Resolver {
    pub fn new(
        http: reqwest::Client,
        indexer_blocklist: HashSet<IndexerId>,
        ip_blocklist: HashSet<IpNetwork>,
        min_versions: Versions,
    ) -> Self {
        Self {
            http,
            blocklist: indexer_blocklist,
            ip_blocker: IpBlocker::new(ip_blocklist),
            min_versions,
        }
    }

    pub async fn resolve(
        &self,
        info: &network_subgraph::types::Indexer,
    ) -> Result<Arc<Indexer>, UnavailableReason> {
        if self.blocklist.contains(&info.id) {
            return Err(UnavailableReason::Blocked);
        }

        let url =
            validate_url(&info.url).map_err(|err| UnavailableReason::NoStatus(err.to_string()))?;

        if let Some(true) = self.ip_blocker.blocked(&url).await {
            return Err(UnavailableReason::Blocked);
        }

        let indexer_service_version = fetch_indexer_service_version(&self.http, &url)
            .await
            .map_err(|indexer_service_version_err| {
                tracing::debug!(indexer = %info.id, %indexer_service_version_err);
                UnavailableReason::NoStatus("failed to fetch version".to_string())
            })?;
        if indexer_service_version < self.min_versions.indexer_service {
            return Err(UnavailableReason::NotSupported(
                "indexer-service version below minimum".into(),
            ));
        }

        let graph_node_version =
            fetch_graph_node_version(&self.http, &url)
                .await
                .map_err(|graph_node_version_err| {
                    tracing::debug!(indexer = %info.id, %graph_node_version_err);
                    UnavailableReason::NoStatus("failed to fetch version".to_string())
                })?;
        if graph_node_version < self.min_versions.graph_node {
            return Err(UnavailableReason::NotSupported(
                "graph-node version below minimum".into(),
            ));
        }

        Ok(Arc::new(Indexer {
            id: info.id,
            url,
            staked_tokens: info.staked_tokens,
            tap_support: indexer_service_version >= "1.0.0-alpha".parse().unwrap(),
        }))
    }
}

// ref: df8e647b-1e6e-422a-8846-dc9ee7e0dcc2
fn validate_url(input: &Option<String>) -> anyhow::Result<Url> {
    let url: Url = input
        .as_ref()
        .ok_or_else(|| anyhow!("missing URL"))?
        .parse()
        .map_err(|err| anyhow!("invalid URL: {err}"))?;
    ensure!(
        url.scheme().starts_with("http"),
        "invalid URL: invalid scheme"
    );
    ensure!(url.host().is_some(), "invalid URL: missing host");
    ensure!(!url.cannot_be_a_base(), "invalid URL: invalid base");
    Ok(url)
}

struct IpBlocker {
    blocklist: HashSet<IpNetwork>,
    dns_resolver: hickory_resolver::TokioAsyncResolver,
    blocked_cache: parking_lot::RwLock<HashMap<Url, bool>>,
}

impl IpBlocker {
    fn new(blocklist: HashSet<IpNetwork>) -> Self {
        Self {
            blocklist,
            dns_resolver: hickory_resolver::TokioAsyncResolver::tokio_from_system_conf()
                .expect("DNS resolver"),
            blocked_cache: Default::default(),
        }
    }

    async fn blocked(&self, url: &Url) -> Option<bool> {
        if let Some(result) = self.blocked_cache.read().get(url) {
            return Some(*result);
        }

        let ips: Vec<IpAddr> = match url.host()? {
            url::Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
            url::Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
            url::Host::Domain(domain) => {
                let timeout = Duration::from_secs(10);
                match tokio::time::timeout(timeout, self.dns_resolver.lookup_ip(domain)).await {
                    Ok(Ok(lookup)) => lookup.into_iter().map(Into::into).collect(),
                    Err(_) => {
                        tracing::debug!(domain, "DNS timeout");
                        return None;
                    }
                    Ok(Err(dns_err)) => {
                        tracing::debug!(domain, %dns_err);
                        return None;
                    }
                }
            }
        };
        let result = ips
            .into_iter()
            .map(|lookup| lookup.to_canonical())
            .any(|ip| self.blocklist.iter().any(|net| net.contains(ip)));
        self.blocked_cache.write().insert(url.clone(), result);
        Some(result)
    }
}

async fn fetch_indexer_service_version(
    http: &reqwest::Client,
    base_url: &Url,
) -> anyhow::Result<Version> {
    let version_url = base_url.join("version").unwrap();
    #[derive(Deserialize)]
    struct Response {
        version: Version,
    }
    let response = http
        .get(version_url)
        .send()
        .await?
        .json::<Response>()
        .await?;
    Ok(response.version)
}

pub async fn fetch_graph_node_version(
    http: &reqwest::Client,
    base_url: &Url,
) -> anyhow::Result<Version> {
    let status_url = base_url.join("status").unwrap();
    let query = "{ version { version } }";
    #[derive(Deserialize)]
    struct Response {
        version: VersionInner,
    }
    #[derive(Deserialize)]
    struct VersionInner {
        version: Version,
    }
    let response: Response = http.post(status_url).send_graphql(query).await??;
    Ok(response.version.version)
}
