use crate::{
    geoip::GeoIP, graphql, indexer_selection::Indexing, network_subgraph::IndexerInfo, prelude::*,
};
use eventuals::EventualExt as _;
use reqwest;
use semver::Version;
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::{spawn, sync::Mutex};
use trust_dns_resolver::TokioAsyncResolver as DNSResolver;
use url::Url;

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
            indexings: indexings_tx,
        }));
        spawn(async move {
            // Joining this eventual with a timer is unnecessary, so long as the Ptr value is
            // updated at regular intervals.
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
                    }
                })
                .forever();
        });
        Data {
            indexings: indexings_rx,
        }
    }

    async fn check_compatibility(&self, indexer: &IndexerInfo) -> Result<(), String> {
        // Apply geoblocking policy
        if let Some(geoip) = &self.geoip {
            let ips = self
                .dns_resolver
                .lookup_ip(indexer.url.as_str())
                .await
                .map_err(|err| err.to_string())?;
            for ip in ips {
                if geoip.is_ip_blocked(ip) {
                    return Err(format!("Geoblocked({})", ip));
                }
            }
        }
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
