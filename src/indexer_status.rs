use crate::{
    geoip::GeoIP, graphql, indexer_selection::Indexing, network_subgraph::IndexerInfo, prelude::*,
};
use eventuals::EventualExt as _;
use futures::future::join_all;
use reqwest;
use semver::Version;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::Arc,
};
use tokio::sync::Mutex;
use trust_dns_resolver::TokioAsyncResolver as DNSResolver;
use url::{Host, Url};

pub struct Data {
    pub indexings: Eventual<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

pub struct IndexingStatus {
    pub network: String,
    pub block: BlockPointer,
}

pub struct Actor {
    min_version: Version,
    geoip: Option<GeoIP>,
    dns_resolver: DNSResolver,
    geoblocking_cache: HashMap<String, Result<(), String>>,
    indexings: EventualWriter<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

impl Actor {
    pub fn create(
        min_version: Version,
        geoip: Option<GeoIP>,
        indexers: Eventual<Ptr<HashMap<Address, IndexerInfo>>>,
    ) -> Data {
        let (indexings_tx, indexings_rx) = Eventual::new();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        let actor = Arc::new(Mutex::new(Actor {
            min_version,
            geoip,
            dns_resolver: DNSResolver::tokio_from_system_conf().unwrap(),
            geoblocking_cache: HashMap::new(),
            indexings: indexings_tx,
        }));
        // Joining this eventual with a timer is unnecessary, so long as the Ptr value is
        // updated at regular intervals. See 4e072dfe-5cb3-4f86-80f6-b64afeb9dcb2
        indexers
            .pipe_async(move |indexers| {
                let actor = actor.clone();
                let client = client.clone();
                async move {
                    let indexings = join_all(indexers.as_ref().clone().into_iter().map(
                        |(indexer, info)| {
                            let actor = actor.clone();
                            let client = client.clone();
                            async move {
                                match Self::indexer_to_indexings(actor, client, indexer, info).await
                                {
                                    Ok(indexings) => indexings,
                                    Err(indexer_status_err) => {
                                        tracing::info!(%indexer_status_err);
                                        vec![]
                                    }
                                }
                            }
                        },
                    ))
                    .await
                    .into_iter()
                    .flatten()
                    .collect::<HashMap<Indexing, IndexingStatus>>();

                    let mut actor = actor.lock().await;
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
        Data {
            indexings: indexings_rx,
        }
    }

    async fn indexer_to_indexings(
        actor: Arc<Mutex<Actor>>,
        client: reqwest::Client,
        indexer: Address,
        info: IndexerInfo,
    ) -> Result<Vec<(Indexing, IndexingStatus)>, String> {
        let version_url = info
            .url
            .join("version")
            .map_err(|err| format!("IndexerVersionError({err})"))?;
        let version = client
            .get(version_url)
            .send()
            .await
            .map_err(|err| format!("IndexerVersionError({err})"))?
            .json::<IndexerVersion>()
            .await
            .map_err(|err| format!("IndexerVersionError({err})"))?
            .version
            .parse::<Version>()
            .map_err(|err| format!("IndexerVersionError({err})"))?;

        let mut actor = actor.lock().await;
        if version < actor.min_version {
            return Err(format!("IndexerVersionBelowMinimum({version})"));
        }
        actor.apply_geoblocking(&info).await?;
        drop(actor);

        Self::query_status(&client, info.url, &indexer)
            .await
            .map_err(|err| format!("IndexerStatusError({err})"))
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
            let host = indexer
                .url
                .host()
                .ok_or_else(|| "host missing in URL".to_string())?;
            let ips = match host {
                Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
                Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
                Host::Domain(domain) => dns_resolver
                    .lookup_ip(domain)
                    .await
                    .map_err(|err| err.to_string())?
                    .into_iter()
                    .collect(),
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
        client: &reqwest::Client,
        url: Url,
        indexer: &Address,
    ) -> Result<Vec<(Indexing, IndexingStatus)>, String> {
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
        let response =
            graphql::query::<IndexerStatusResponse, _>(client, url, &json!({ "query": query }))
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
                let indexing = Indexing {
                    indexer: indexer.clone(),
                    deployment: status.subgraph,
                };
                let chain = &status.chains.get(0)?;
                let status = IndexingStatus {
                    network: chain.network.clone(),
                    block: BlockPointer {
                        number: chain.latest_block.number.parse().ok()?,
                        hash: chain.latest_block.hash.clone(),
                    },
                };
                Some((indexing, status))
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
