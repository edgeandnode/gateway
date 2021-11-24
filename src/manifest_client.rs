use crate::{ipfs_client::*, prelude::*};
use eventuals::EventualExt;
use im;
use serde::Deserialize;
use serde_yaml;
use std::sync::Arc;
use tokio::time::sleep;

pub struct SubgraphInfo {
    pub id: SubgraphDeploymentID,
    pub network: String,
    pub features: Vec<String>,
}

pub type SubgraphInfoMap =
    Eventual<Ptr<im::HashMap<SubgraphDeploymentID, Eventual<Ptr<SubgraphInfo>>>>>;

pub fn create(
    ipfs_client: Arc<IPFSClient>,
    subgraphs: Eventual<Vec<SubgraphDeploymentID>>,
) -> SubgraphInfoMap {
    let manifests = im::HashMap::new();
    subgraphs.map(move |subgraphs| {
        let ipfs_client = ipfs_client.clone();
        let mut manifests = manifests.clone();
        async move {
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
                let (mut writer, reader) = Eventual::new();
                tokio::spawn({
                    let client = ipfs_client.clone();
                    async move {
                        loop {
                            match fetch_manifest(&client, deployment).await {
                                Ok(response) => {
                                    writer.write(Ptr::new(response));
                                    break;
                                }
                                Err((deployment, manifest_fetch_err)) => {
                                    tracing::warn!(%deployment, %manifest_fetch_err);
                                    sleep(Duration::from_secs(20)).await;
                                }
                            }
                        }
                    }
                });
                manifests.insert(deployment, reader);
            }
            Ptr::new(manifests)
        }
    })
}

pub async fn fetch_manifest(
    client: &IPFSClient,
    id: SubgraphDeploymentID,
) -> Result<SubgraphInfo, (SubgraphDeploymentID, String)> {
    let payload = client
        .cat(&id.ipfs_hash())
        .await
        .map_err(|err| (id, err.to_string()))?;
    let manifest =
        serde_yaml::from_str::<SubgraphManifest>(&payload).map_err(|err| (id, err.to_string()))?;
    // We are assuming that all `dataSource.network` fields are identical.
    // This is guaranteed for now.
    let network = manifest
        .data_sources
        .into_iter()
        .filter_map(|data_source| data_source.network)
        .next()
        .ok_or_else(|| (id, "Network not found".to_string()))?;
    Ok(SubgraphInfo {
        id,
        network,
        features: manifest.features,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubgraphManifest {
    pub data_sources: Vec<DataSource>,
    #[serde(default)]
    pub features: Vec<String>,
}

#[derive(Deserialize)]
pub struct DataSource {
    pub network: Option<String>,
}
