use crate::{
    geoip::GeoIP, graphql, indexer_selection::Indexing, network_subgraph::IndexerInfo, prelude::*,
};
use eventuals::EventualExt as _;
use reqwest;
use semver::Version;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    net::IpAddr
};
use tokio::{spawn, sync::Mutex};
use trust_dns_resolver::TokioAsyncResolver as DNSResolver;
use url::{Url, Host};

pub struct Data {
    pub indexings: Eventual<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

pub struct IndexingStatus {
    pub network: String,
    pub block: BlockPointer,
}

pub struct Actor {
    client: reqwest::Client,
    min_version: Version,
    geoip: Option<GeoIP>,
    dns_resolver: DNSResolver,
    geoblocking_cache: HashMap<String, Result<(), String>>,
    indexings: EventualWriter<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

impl Actor {
    pub fn create(
        client: reqwest::Client,
        min_version: Version,
        geoip: Option<GeoIP>,
        indexers: Eventual<Ptr<HashMap<Address, IndexerInfo>>>,
    ) -> Data {
        let (indexings_tx, indexings_rx) = Eventual::new();
        let actor = Arc::new(Mutex::new(Actor {
            client,
            min_version,
            geoip,
            dns_resolver: DNSResolver::tokio_from_system_conf().unwrap(),
            geoblocking_cache: HashMap::new(),
            indexings: indexings_tx,
        }));
        spawn(async move {
            // Joining this eventual with a timer is unnecessary, so long as the Ptr value is
            // updated at regular intervals. See 4e072dfe-5cb3-4f86-80f6-b64afeb9dcb2
            indexers
                .pipe_async(move |indexers| {
                    let actor = actor.clone();
                    async move {
                        let mut actor = actor.lock().await;
                        let mut indexings = HashMap::<Indexing, IndexingStatus>::new();
                        for (indexer, info) in indexers.iter() {
                            if let Err(reason) = actor.check_compatibility(info).await {
                                tracing::info!(
                                    %indexer,
                                    url = %info.url,
                                    %reason,
                                    "incompatible indexer",
                                );
                                continue;
                            }
                            let result = actor.query_status(info.url.clone()).await;
                            let response = match result {
                                Ok(response) => response,
                                Err(indexer_status_err) => {
                                    tracing::debug!(%indexer_status_err);
                                    continue;
                                }
                            };
                            for (deployment, status) in response {
                                indexings.insert(
                                    Indexing {
                                        deployment,
                                        indexer: indexer.clone(),
                                    },
                                    status,
                                );
                            }
                        }
                        actor.indexings.write(Ptr::new(indexings));

                        // Remove unused entries from geoblocking cache.
                        let urls = indexers
                            .values()
                            .map(|info| info.url.as_str())
                            .collect::<HashSet<&str>>();
                        let cache = actor
                            .geoblocking_cache
                            .drain()
                            .filter(|(key, _)| urls.contains(key.as_str()))
                            .collect();
                        actor.geoblocking_cache = cache;
                    }
                })
                .forever();
        });
        Data {
            indexings: indexings_rx,
        }
    }

    async fn check_compatibility(&mut self, indexer: &IndexerInfo) -> Result<(), String> {
        self.apply_geoblocking(indexer).await?;

        // Check for indexer version
        let version_url = indexer.url.join("version").map_err(|err| err.to_string())?;
        let version = self
            .client
            .get(version_url)
            .send()
            .await
            .map_err(|err| err.to_string())?
            .json::<IndexerVersion>()
            .await
            .map_err(|err| err.to_string())?
            .version
            .parse::<Version>()
            .map_err(|err| err.to_string())?;
        if version < self.min_version {
            return Err(format!("Version({})", version));
        }

        Ok(())
    }

    async fn apply_geoblocking(&mut self, indexer: &IndexerInfo) -> Result<(), String> {
        let geoip = match &self.geoip {
            Some(geoip) => geoip,
            None => return Ok(()),
        };
        let key = indexer.url.as_str();
        if let Some(result) = self.geoblocking_cache.get(key) {
            return result.clone();
        }
        async fn apply_geoblocking_inner(
            dns_resolver: &DNSResolver,
            geoip: &GeoIP,
            indexer: &IndexerInfo,
        ) -> Result<(), String> {
            let host = indexer.url.host().ok_or_else(|| "host missing in URL".to_string())?;
            let ips = match host {
                Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
                Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
                Host::Domain(domain) => dns_resolver
                .lookup_ip(domain)
                .await
                .map_err(|err| err.to_string())?.into_iter().collect()
            };
            for ip in ips {
                if geoip.is_ip_blocked(ip) {
                    return Err(format!("Geoblocked({})", ip));
                }
            }
            Ok(())
        }
        let result = apply_geoblocking_inner(&self.dns_resolver, geoip, indexer).await;
        self.geoblocking_cache
            .insert(key.to_string(), result.clone());
        result
    }

    async fn query_status(
        &self,
        url: Url,
    ) -> Result<Vec<(SubgraphDeploymentID, IndexingStatus)>, String> {
        let url = url.join("status").map_err(|err| err.to_string())?;
        let query = r#"{
            indexingStatuses(subgraphs: []) {
                subgraph
                chains {
                    network
                    ... on EthereumIndexingStatus { latestBlock { number hash } }
                }
            }
        }"#;
        let response = graphql::query::<IndexerStatusResponse, _>(
            &self.client,
            url,
            &json!({ "query": query }),
        )
        .await?;
        let response = response.data.ok_or_else(|| {
            response
                .errors
                .unwrap_or_default()
                .into_iter()
                .map(|err| err.message)
                .collect::<Vec<String>>()
                .join(", ")
        })?;
        Ok(response
            .indexing_statuses
            .into_iter()
            .filter_map(|status| {
                let deployment = status.subgraph;
                let chain = &status.chains.get(0)?;
                let status = IndexingStatus {
                    network: chain.network.clone(),
                    block: BlockPointer {
                        number: chain.latest_block.number.parse().ok()?,
                        hash: chain.latest_block.hash.clone(),
                    },
                };
                Some((deployment, status))
            })
            .collect())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexerStatusResponse {
    indexing_statuses: Vec<IndexingStatusResponse>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexingStatusResponse {
    subgraph: SubgraphDeploymentID,
    chains: Vec<ChainStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChainStatus {
    network: String,
    latest_block: BlockStatus,
}

#[derive(Deserialize)]
struct BlockStatus {
    number: String,
    hash: Bytes32,
}

#[derive(Deserialize)]
struct IndexerVersion {
    version: String,
}
