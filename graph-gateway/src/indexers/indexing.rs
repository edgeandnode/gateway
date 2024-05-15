use std::{collections::HashMap, sync::Arc};

use alloy_primitives::Address;
use anyhow::{anyhow, ensure};
use cost_model::CostModel;
use eventuals::{Eventual, EventualExt as _, EventualWriter, Ptr};
use futures::future::join_all;
use gateway_common::types::Indexing;
use gateway_framework::{network::discovery::Status, topology::network::Deployment};
use semver::Version;
use thegraph_core::types::DeploymentId;
use tokio::sync::Mutex;
use toolshed::epoch_cache::EpochCache;
use url::Url;

use crate::indexers::{cost_models, indexing_statuses, version};

pub async fn statuses(
    deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    client: reqwest::Client,
    min_graph_node_version: Version,
    min_indexer_service_version: Version,
) -> Eventual<Ptr<HashMap<Indexing, Status>>> {
    let (indexing_statuses_tx, indexing_statuses_rx) = Eventual::new();
    let actor: &'static Mutex<Actor> = Box::leak(Box::new(Mutex::new(Actor {
        min_graph_node_version,
        min_indexer_service_version,
        cost_model_cache: EpochCache::new(),
        indexing_statuses: Default::default(),
        indexing_statuses_tx,
    })));

    // Joining this eventual with a timer is unnecessary, as long as deployments update at a regular
    // interval. See 4e072dfe-5cb3-4f86-80f6-b64afeb9dcb2
    deployments
        .pipe_async(move |deployments| {
            let client = client.clone();
            async move { update_statuses(actor, client, &deployments).await }
        })
        .forever();

    // Wait for first sync before proceeding.
    if indexing_statuses_rx.value().await.is_err() {
        panic!("Failed to await indexing_statuses");
    }

    indexing_statuses_rx
}

struct Actor {
    min_graph_node_version: Version,
    min_indexer_service_version: Version,
    cost_model_cache: EpochCache<CostModelSource, Result<Ptr<CostModel>, String>, 2>,
    indexing_statuses: HashMap<Indexing, Status>,
    indexing_statuses_tx: EventualWriter<Ptr<HashMap<Indexing, Status>>>,
}

async fn update_statuses(
    actor: &'static Mutex<Actor>,
    client: reqwest::Client,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
) {
    // There can only be one URL per indexer entity in the network subgraph
    let mut indexers: HashMap<Address, (Url, Vec<DeploymentId>)> = Default::default();
    for deployment in deployments.values() {
        for indexer in deployment.indexers.values() {
            let (_, deployments) = indexers
                .entry(indexer.id)
                .or_insert_with(|| (indexer.url.clone(), vec![]));
            deployments.push(deployment.id);
        }
    }

    let statuses = join_all(
        indexers
            .into_iter()
            .map(move |(indexer, (url, deployments))| {
                let client = client.clone();
                async move {
                    match update_indexer(actor, &client, indexer, url, deployments).await {
                        Ok(indexings) => indexings,
                        Err(indexer_status_err) => {
                            tracing::warn!(?indexer, %indexer_status_err);
                            vec![]
                        }
                    }
                }
            }),
    )
    .await
    .into_iter()
    .flatten();

    let mut actor = actor.lock().await;
    for (indexing, status) in statuses {
        actor.indexing_statuses.insert(indexing, status);
    }
    let statuses = actor.indexing_statuses.clone();
    actor.indexing_statuses_tx.write(Ptr::new(statuses));

    actor.cost_model_cache.increment_epoch();
}

async fn update_indexer(
    actor: &'static Mutex<Actor>,
    client: &reqwest::Client,
    indexer: Address,
    url: Url,
    deployments: Vec<DeploymentId>,
) -> anyhow::Result<Vec<(Indexing, Status)>> {
    let version_url = url
        .join("version")
        .map_err(|err| anyhow!("IndexerVersionError({err})"))?;
    let service_version = version::query_indexer_service_version(client, version_url)
        .await
        .map_err(|err| anyhow::anyhow!("IndexerVersionError({err})"))?;
    let status_url = url.join("status")?;
    let graph_node_version = version::query_graph_node_version(client, status_url).await;

    let locked_actor = actor.lock().await;
    ensure!(
        service_version >= locked_actor.min_indexer_service_version,
        "IndexerServiceVersionBelowMinimum({service_version})",
    );
    // TODO: Strongly enforce graph-node version, by removing this statement, after more indexers
    // update their indexer-service.
    let graph_node_version =
        graph_node_version.unwrap_or_else(|_| locked_actor.min_graph_node_version.clone());
    ensure!(
        graph_node_version >= locked_actor.min_graph_node_version,
        "GraphNodeVersionBelowMinimum({graph_node_version})",
    );
    drop(locked_actor);

    query_status(actor, client, indexer, url, deployments, service_version)
        .await
        .map_err(|err| anyhow!("IndexerStatusError({err})"))
}

async fn query_status(
    actor: &'static Mutex<Actor>,
    client: &reqwest::Client,
    indexer: Address,
    url: Url,
    deployments: Vec<DeploymentId>,
    version: Version,
) -> anyhow::Result<Vec<(Indexing, Status)>> {
    let status_url = url.join("status")?;
    let statuses = indexing_statuses::query(client, status_url, &deployments).await?;

    let cost_url = url.join("cost")?;
    let deployments: Vec<DeploymentId> = statuses.iter().map(|stat| stat.subgraph).collect();
    let cost_models = cost_models::query(client, cost_url, &deployments)
        .await
        .unwrap_or_default();

    let mut actor = actor.lock().await;
    let mut cost_models = cost_models
        .into_iter()
        .filter_map(|src| {
            let cost_model =
                match compile_cost_model(&mut actor.cost_model_cache, src.model, src.variables) {
                    Ok(cost_model) => cost_model,
                    Err(cost_model_compile_err) => {
                        tracing::debug!(
                            %cost_model_compile_err,
                            ?indexer,
                            deployment = %src.deployment,
                        );
                        return None;
                    }
                };
            Some((src.deployment, cost_model))
        })
        .collect::<HashMap<DeploymentId, Ptr<CostModel>>>();
    drop(actor);

    // TODO: Minimum indexer version supporting Scalar TAP (temporary, as non-TAP Scalar is deprecated)
    let min_scalar_tap_version: Version = "1.0.0-alpha".parse().unwrap();
    let legacy_scalar = version < min_scalar_tap_version;

    Ok(statuses
        .into_iter()
        .filter_map(|status| {
            let indexing = Indexing {
                indexer,
                deployment: status.subgraph,
            };
            let chain = &status.chains.first()?;
            let cost_model = cost_models.remove(&indexing.deployment);
            let block_status = chain.latest_block.as_ref()?;
            let status = Status {
                block: block_status.number,
                min_block: chain.earliest_block.as_ref().map(|b| b.number),
                cost_model,
                legacy_scalar,
            };
            Some((indexing, status))
        })
        .collect())
}

fn compile_cost_model(
    cache: &mut EpochCache<CostModelSource, Result<Ptr<CostModel>, String>, 2>,
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
    cache
        .get_or_insert(src, |src| {
            CostModel::compile(&src.model, &src.variables)
                .map(Ptr::new)
                .map_err(|err| err.to_string())
        })
        .clone()
}

#[derive(Eq, Hash, PartialEq)]
struct CostModelSource {
    model: String,
    variables: String,
}
