use crate::geoip::GeoIP;
use crate::subgraph_client::graphql_query;
use eventuals::EventualExt as _;
use futures::future::join_all;
use indexer_selection::cost_model::CostModel;
use indexer_selection::{IndexerInfo, Indexing};
use prelude::{epoch_cache::EpochCache, graphql, url::url::Host, *};
use semver::Version;
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, net::IpAddr, sync::Arc};
use tokio::sync::Mutex;
use trust_dns_resolver::TokioAsyncResolver as DNSResolver;

pub struct Data {
    pub indexings: Eventual<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

pub struct IndexingStatus {
    pub network: String,
    pub block: BlockPointer,
    pub min_block: Option<u64>,
    pub cost_model: Option<Ptr<CostModel>>,
}

pub struct Actor {
    min_version: Version,
    geoip: Option<GeoIP>,
    dns_resolver: DNSResolver,
    geoblocking_cache: EpochCache<String, Result<(), String>, 2>,
    cost_model_cache: EpochCache<CostModelSource, Result<Ptr<CostModel>, String>, 2>,
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
        indexers: Eventual<Ptr<HashMap<Address, Arc<IndexerInfo>>>>,
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
            geoblocking_cache: EpochCache::new(),
            cost_model_cache: EpochCache::new(),
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
                                match Self::indexer_to_indexings(actor, client, indexer, &info)
                                    .await
                                {
                                    Ok(indexings) => indexings,
                                    Err(indexer_status_err) => {
                                        tracing::info!(%indexer, %indexer_status_err);
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
                    actor.geoblocking_cache.increment_epoch();
                    actor.cost_model_cache.increment_epoch();
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
        info: &IndexerInfo,
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
        locked_actor.apply_geoblocking(info).await?;
        drop(locked_actor);

        Self::query_status(&client, actor, info.url.clone(), &indexer)
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
                    return Err(format!("Geoblocked({ip})"));
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
                    latestBlock { number hash }
                    earliestBlock { number hash }
                }
            }
        }"# });
        let statuses =
            graphql_query::<IndexerStatusResponse>(client, status_url.into(), &status_query, None)
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
        let cost_models =
            graphql_query::<CostModelResponse>(client, cost_url.into(), &cost_query, None)
                .await
                .and_then(graphql::http::Response::unpack)
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
            .collect::<HashMap<DeploymentId, Ptr<CostModel>>>();
        drop(actor);

        Ok(statuses
            .into_iter()
            .filter_map(|status| {
                let indexing = Indexing {
                    indexer: *indexer,
                    deployment: status.subgraph,
                };
                let chain = &status.chains.get(0)?;
                let cost_model = cost_models.remove(&indexing.deployment);
                let block_status = chain.latest_block.as_ref()?;
                let status = IndexingStatus {
                    network: chain.network.clone(),
                    block: BlockPointer {
                        number: block_status.number.parse().ok()?,
                        hash: block_status.hash,
                    },
                    min_block: chain
                        .earliest_block
                        .as_ref()
                        .and_then(|b| b.number.parse::<u64>().ok()),
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
    ) -> Result<Ptr<CostModel>, String> {
        if model.len() > (1 << 16) {
            return Err("CostModelTooLarge".into());
        }
        let src = CostModelSource {
            model,
            variables: variables.unwrap_or_default(),
        };
        self.cost_model_cache
            .get_or_insert(src, |src| {
                CostModel::compile(&src.model, &src.variables)
                    .map(Ptr::new)
                    .map_err(|err| err.to_string())
            })
            .clone()
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
    subgraph: DeploymentId,
    chains: Vec<ChainStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChainStatus {
    network: String,
    latest_block: Option<BlockStatus>,
    earliest_block: Option<BlockStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CostModelResponse {
    cost_models: Vec<CostModelSourceResponse>,
}

#[derive(Deserialize)]
struct CostModelSourceResponse {
    deployment: DeploymentId,
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
