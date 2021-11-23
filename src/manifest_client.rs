use crate::{ipfs_client::*, prelude::*};
use eventuals::EventualExt;
use futures::{stream, StreamExt};
use im;
use serde::Deserialize;
use serde_yaml;
use std::sync::Arc;

pub struct SubgraphInfo {
    pub id: SubgraphDeploymentID,
    pub network: String,
    pub features: Vec<String>,
}

impl Eq for SubgraphInfo {}
impl PartialEq for SubgraphInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

pub fn create(
    ipfs_client: Arc<IPFSClient>,
    subgraphs: Eventual<Vec<SubgraphDeploymentID>>,
    max_concurrent: usize,
) -> Eventual<im::HashMap<SubgraphDeploymentID, Arc<SubgraphInfo>>> {
    let manifests = im::HashMap::new();
    subgraphs.map(move |subgraphs| {
        let ipfs_client = ipfs_client.clone();
        let mut manifests = manifests.clone();
        async move {
            let unresolved = subgraphs
                .into_iter()
                .filter(|id| !manifests.contains_key(id))
                .collect::<Vec<SubgraphDeploymentID>>();
            let results = stream::iter(unresolved)
                .map(|id| {
                    let ipfs_client = &ipfs_client;
                    async move { fetch_manifest(ipfs_client, id).await }
                })
                .buffer_unordered(max_concurrent)
                .collect::<Vec<Result<SubgraphInfo, (SubgraphDeploymentID, String)>>>()
                .await;
            for result in results {
                match result {
                    Ok(manifest) => {
                        manifests.insert(manifest.id, Arc::new(manifest));
                    }
                    Err((deployment, manifest_fetch_err)) => {
                        tracing::warn!(%deployment, %manifest_fetch_err);
                    }
                };
            }
            manifests
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
