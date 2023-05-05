use crate::{ipfs, network_subgraph};
use futures_util::future::join_all;
use prelude::{anyhow::anyhow, eventuals::EventualExt as _, tokio::sync::RwLock, *};
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

/// Representation of the graph network being used to serve queries
#[derive(Clone)]
pub struct GraphNetwork {
    pub subgraphs: Eventual<Ptr<HashMap<SubgraphId, Subgraph>>>,
    pub deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
}

/// In an effort to keep the ownership structure a simple tree, this only contains the info required
/// to resolve queries by `SubgraphId` into the relevant deployments. Therefore, there is no need
/// for a query by `DeploymentId` to interact with this.
pub struct Subgraph {
    pub deployments: Vec<Arc<Deployment>>,
}

pub struct Deployment {
    pub id: DeploymentId,
    pub manifest: Arc<Manifest>,
    pub allocations: Vec<Allocation>,
    /// A deployment may be associated with multiple subgraphs.
    pub subgraphs: BTreeSet<SubgraphId>,
    /// Indicates that all associated subgraphs have been migrated to L2, and should not be served
    /// directly by this gateway.
    pub migrated_to_l2: bool,
}

pub struct Allocation {
    pub id: Address,
    pub allocated_tokens: GRT,
    pub indexer: Indexer,
}

pub struct Indexer {
    pub id: Address,
    pub url: Url,
    pub staked_tokens: GRT,
}

pub struct Manifest {
    pub network: String,
    pub features: Vec<String>,
    pub min_block: u64,
}

impl GraphNetwork {
    pub async fn new(
        subgraphs: Eventual<Ptr<Vec<network_subgraph::Subgraph>>>,
        ipfs: Arc<ipfs::Client>,
    ) -> Self {
        let manifest_cache: &'static RwLock<ManifestCache> =
            Box::leak(Box::new(RwLock::new(ManifestCache {
                ipfs,
                cache: HashMap::new(),
            })));

        let subgraphs = subgraphs.map(move |subgraphs| async move {
            Ptr::new(Self::subgraphs(&subgraphs, manifest_cache).await)
        });
        let deployments = subgraphs.clone().map(|subgraphs| async move {
            subgraphs
                .values()
                .flat_map(|subgraph| &subgraph.deployments)
                .map(|deployment| (deployment.id, deployment.clone()))
                .collect::<HashMap<DeploymentId, Arc<Deployment>>>()
                .into()
        });

        // Return only after eventuals have values, to avoid serving client queries prematurely.
        if deployments.value().await.is_err() {
            panic!("Failed to await Graph network topology");
        }

        Self {
            subgraphs,
            deployments,
        }
    }

    async fn subgraphs(
        subgraphs: &[network_subgraph::Subgraph],
        manifest_cache: &'static RwLock<ManifestCache>,
    ) -> HashMap<SubgraphId, Subgraph> {
        join_all(subgraphs.iter().map(|subgraph| async move {
            let deployments = join_all(
                subgraph
                    .versions
                    .iter()
                    .map(|version| Self::deployment(subgraphs, version, manifest_cache)),
            )
            .await
            .into_iter()
            .flatten()
            .collect();
            (subgraph.id, Subgraph { deployments })
        }))
        .await
        .into_iter()
        .collect()
    }

    async fn deployment(
        subgraphs: &[network_subgraph::Subgraph],
        version: &network_subgraph::SubgraphVersion,
        manifest_cache: &'static RwLock<ManifestCache>,
    ) -> Option<Arc<Deployment>> {
        let id = version.subgraph_deployment.id;
        let manifest = ManifestCache::get(manifest_cache, &version.subgraph_deployment.id).await?;
        let subgraphs = subgraphs
            .iter()
            .filter(|subgraph| {
                subgraph
                    .versions
                    .iter()
                    .any(|v| v.subgraph_deployment.id == id)
            })
            .map(|subgraph| subgraph.id)
            .collect();
        let allocations = version
            .subgraph_deployment
            .allocations
            .iter()
            .filter_map(|allocation| {
                Some(Allocation {
                    id: allocation.id,
                    allocated_tokens: allocation.allocated_tokens.change_precision(),
                    indexer: Indexer {
                        id: allocation.indexer.id,
                        url: allocation.indexer.url.parse().ok()?,
                        staked_tokens: allocation.indexer.staked_tokens.change_precision(),
                    },
                })
            })
            .collect();
        Some(Arc::new(Deployment {
            id,
            manifest,
            subgraphs,
            allocations,
            migrated_to_l2: false, // TODO
        }))
    }
}

struct ManifestCache {
    ipfs: Arc<ipfs::Client>,
    cache: HashMap<DeploymentId, Arc<Manifest>>,
}

impl ManifestCache {
    async fn get(cache: &RwLock<Self>, deployment: &DeploymentId) -> Option<Arc<Manifest>> {
        let read = cache.read().await;
        if let Some(manifest) = read.cache.get(deployment) {
            return Some(manifest.clone());
        }
        let ipfs = read.ipfs.clone();
        drop(read);

        let manifest = match Self::cat_manifest(&ipfs, deployment).await {
            Ok(manifest) => Arc::new(manifest),
            Err(manifest_err) => {
                tracing::warn!(%deployment, %manifest_err);
                return None;
            }
        };

        let mut write = cache.write().await;
        write.cache.insert(*deployment, manifest.clone());
        Some(manifest)
    }

    async fn cat_manifest(
        ipfs: &ipfs::Client,
        deployment: &DeploymentId,
    ) -> anyhow::Result<Manifest> {
        // Subgraph manifest schema:
        // https://github.com/graphprotocol/graph-node/blob/master/docs/subgraph-manifest.md
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct ManifestSrc {
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

        let payload = ipfs.cat(&deployment.ipfs_hash()).await?;
        let manifest = serde_yaml::from_str::<ManifestSrc>(&payload)?;
        let min_block = manifest
            .data_sources
            .iter()
            .map(|data_source| data_source.source.start_block.unwrap_or(0))
            .min()
            .unwrap_or(0);
        // We are assuming that all `dataSource.network` fields are identical.
        let network = manifest
            .data_sources
            .into_iter()
            .map(|data_source| data_source.network)
            .next()
            .ok_or_else(|| anyhow!("Network not found"))?;
        Ok(Manifest {
            network,
            min_block,
            features: manifest.features,
        })
    }
}
