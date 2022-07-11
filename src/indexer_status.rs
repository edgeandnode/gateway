use crate::{
    geoip::GeoIP, graphql, indexer_selection::Indexing, network_subgraph::IndexerInfo, prelude::*,
};
use cost_model::{self, CostModel};
use eventuals::EventualExt as _;
use futures::future::join_all;
use reqwest;
use semver::Version;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
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
    pub cost_model: Option<Arc<CostModel>>,
}

pub struct Actor {
    min_version: Version,
    geoip: Option<GeoIP>,
    dns_resolver: DNSResolver,
    geoblocking_cache: HashMap<String, Result<(), String>>,
    cost_model_cache: HashMap<CostModelSource, Result<Arc<CostModel>, String>>,
    indexings: EventualWriter<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

#[derive(Eq, Hash, PartialEq)]
struct CostModelSource {
    model: String,
    variables: String,
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
            cost_model_cache: HashMap::new(),
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
                    let used_urls = indexers
                        .values()
                        .map(|info| info.url.as_str())
                        .collect::<HashSet<&str>>();
                    let cache = actor
                        .geoblocking_cache
                        .drain()
                        .filter(|(key, _)| used_urls.contains(key.as_str()))
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

        let mut locked_actor = actor.lock().await;
        if version < locked_actor.min_version {
            return Err(format!("IndexerVersionBelowMinimum({version})"));
        }
        locked_actor.apply_geoblocking(&info).await?;
        drop(locked_actor);

        Self::query_status(&client, actor, info.url, &indexer)
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
        actor: Arc<Mutex<Actor>>,
        url: Url,
        indexer: &Address,
    ) -> Result<Vec<(Indexing, IndexingStatus)>, String> {
        let status_url = url.join("status").map_err(|err| err.to_string())?;
        let status_query = json!({ "query": r#"{
            indexingStatuses(subgraphs: []) {
                subgraph
                chains {
                    network
                    ... on EthereumIndexingStatus { latestBlock { number hash } }
                }
            }
        }"# });
        let statuses =
            graphql::query::<IndexerStatusResponse, _>(client, status_url, &status_query)
                .await?
                .unpack()?
                .indexing_statuses;

        let cost_url = url.join("cost").map_err(|err| err.to_string())?;
        let deployments = statuses
            .iter()
            .map(|stat| stat.subgraph.to_string())
            .collect::<Vec<String>>();
        let cost_query = json!({
            "query": r#"query costModels($deployments: [String!]!) {
                costModels(deployments: $deployments) {
                    deployment
                    model
                    variables
                }
            }"#,
            "variables": { "deployments": deployments },
        });
        let cost_models = graphql::query::<CostModelResponse, _>(client, cost_url, &cost_query)
            .await
            .and_then(graphql::Response::unpack)
            .map(|cost_models| cost_models.cost_models)
            .unwrap_or_default();

        let mut actor = actor.lock().await;
        let mut cost_models = cost_models
            .into_iter()
            .filter_map(|src| {
                let cost_model = match actor.compile_cost_model(src.model, src.variables) {
                    Ok(cost_model) => cost_model,
                    Err(cost_model_compile_err) => {
                        tracing::debug!(%cost_model_compile_err, %indexer, deployment = %src.deployment);
                        return None;
                    }
                };
                Some((src.deployment, cost_model))
            })
            .collect::<HashMap<SubgraphDeploymentID, Arc<CostModel>>>();
        drop(actor);

        Ok(statuses
            .into_iter()
            .filter_map(|status| {
                let indexing = Indexing {
                    indexer: indexer.clone(),
                    deployment: status.subgraph,
                };
                let chain = &status.chains.get(0)?;
                let cost_model = cost_models.remove(&indexing.deployment);
                let status = IndexingStatus {
                    network: chain.network.clone(),
                    block: BlockPointer {
                        number: chain.latest_block.number.parse().ok()?,
                        hash: chain.latest_block.hash.clone(),
                    },
                    cost_model,
                };
                Some((indexing, status))
            })
            .collect())
    }

    fn compile_cost_model(
        &mut self,
        model: String,
        variables: Option<String>,
    ) -> Result<Arc<CostModel>, String> {
        // TODO: This cache should have an eviction strategy.
        let src = CostModelSource {
            model,
            variables: variables.unwrap_or_default(),
        };
        match self.cost_model_cache.entry(src) {
            Entry::Occupied(result) => result.get().clone(),
            Entry::Vacant(entry) => {
                let src = entry.key();
                let result = CostModel::compile(&src.model, &src.variables)
                    .map(Arc::new)
                    .map_err(|err| err.to_string());
                entry.insert(result.clone());
                result
            }
        }
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
#[serde(rename_all = "camelCase")]
struct CostModelResponse {
    cost_models: Vec<CostModelSourceResponse>,
}

#[derive(Deserialize)]
struct CostModelSourceResponse {
    deployment: SubgraphDeploymentID,
    model: String,
    variables: Option<String>,
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
