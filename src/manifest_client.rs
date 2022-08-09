use crate::{ipfs_client::*, prelude::*, subgraph_deployments::SubgraphDeployments};
use eventuals::EventualExt;
use im;
use serde::Deserialize;
use serde_yaml;
use std::sync::Arc;
use tokio::{sync::Mutex, time::sleep};

#[derive(Debug)]
pub struct SubgraphInfo {
    pub id: SubgraphID,
    pub deployment: SubgraphDeploymentID,
    pub network: String,
    pub features: Vec<String>,
    pub min_block: u64,
}

pub type SubgraphInfoMap =
    Eventual<Ptr<im::HashMap<SubgraphDeploymentID, Eventual<Ptr<SubgraphInfo>>>>>;

pub fn create(
    ipfs_client: Arc<IPFSClient>,
    subgraph_deployments: SubgraphDeployments,
    subgraphs: Eventual<Vec<SubgraphDeploymentID>>,
) -> SubgraphInfoMap {
    let manifests: Arc<Mutex<im::HashMap<SubgraphDeploymentID, Eventual<Ptr<SubgraphInfo>>>>> =
        Arc::default();
    subgraphs.map(move |subgraphs| {
        let ipfs_client = ipfs_client.clone();
        let manifests = manifests.clone();
        let subgraph_deployments = subgraph_deployments.clone();
        async move {
            let mut manifests = manifests.lock().await;
            // Remove deployments not present in updated set
            let stale_deployments = manifests
                .keys()
                .filter(|id| !subgraphs.contains(id))
                .cloned()
                .collect::<Vec<SubgraphDeploymentID>>();
            for deployment in stale_deployments {
                manifests.remove(&deployment);
            }

            let unresolved = subgraphs
                .into_iter()
                .filter(|id| !manifests.contains_key(id))
                .collect::<Vec<SubgraphDeploymentID>>();
            for deployment in unresolved {
                let client = ipfs_client.clone();
                let subgraph_deployments = subgraph_deployments.clone();
                let info = Eventual::spawn(move |mut writer| async move {
                    let subgraph = match subgraph_deployments.deployment_subgraph(&deployment) {
                        Some(subgraph) => subgraph,
                        None => {
                            tracing::error!(%deployment, "deployment missing supgraph");
                            return Err(eventuals::Closed);
                        }
                    };
                    loop {
                        match fetch_manifest(&client, subgraph, deployment).await {
                            Ok(response) => {
                                writer.write(Ptr::new(response));
                                return Err(eventuals::Closed);
                            }
                            Err((deployment, manifest_fetch_err)) => {
                                tracing::warn!(%deployment, %manifest_fetch_err);
                                sleep(Duration::from_secs(20)).await;
                            }
                        }
                    }
                });
                manifests.insert(deployment, info);
            }
            Ptr::new(manifests.clone())
        }
    })
}

pub async fn fetch_manifest(
    client: &IPFSClient,
    subgraph: SubgraphID,
    deployment: SubgraphDeploymentID,
) -> Result<SubgraphInfo, (SubgraphDeploymentID, String)> {
    let payload = client
        .cat(&deployment.ipfs_hash())
        .await
        .map_err(|err| (deployment, err.to_string()))?;
    let manifest = serde_yaml::from_str::<SubgraphManifest>(&payload)
        .map_err(|err| (deployment, err.to_string()))?;
    let min_block = manifest
        .data_sources
        .iter()
        .map(|data_source| data_source.source.start_block.unwrap_or(0))
        .min()
        .unwrap_or(0);
    // We are assuming that all `dataSource.network` fields are identical.
    // This is guaranteed for now.
    let network = manifest
        .data_sources
        .into_iter()
        .map(|data_source| data_source.network)
        .next()
        .ok_or_else(|| (deployment, "Network not found".to_string()))?;
    Ok(SubgraphInfo {
        id: subgraph,
        deployment,
        network,
        min_block,
        features: manifest.features,
    })
}

// Subgraph manifest schema:
// https://github.com/graphprotocol/graph-node/blob/master/docs/subgraph-manifest.md

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubgraphManifest {
    pub data_sources: Vec<DataSource>,
    #[serde(default)]
    pub features: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSource {
    pub network: String,
    pub source: EthereumContractSource,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthereumContractSource {
    pub start_block: Option<u64>,
}
