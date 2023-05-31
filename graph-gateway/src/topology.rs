use crate::{ipfs, network_subgraph};
use chrono::Utc;
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
#[derive(Clone)]
pub struct Subgraph {
    pub deployments: Vec<Arc<Deployment>>,
    /// Indicates that the subgraph has been transferred to L2, and should not be served
    /// directly by this gateway.
    pub transferred_to_l2: bool,
}

pub struct Deployment {
    pub id: DeploymentId,
    pub manifest: Arc<Manifest>,
    pub version: Option<Arc<semver::Version>>,
    pub allocations: Vec<Allocation>,
    /// A deployment may be associated with multiple subgraphs.
    pub subgraphs: BTreeSet<SubgraphId>,
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
        l2_transfer_delay: Option<chrono::Duration>,
    ) -> Self {
        let cache: &'static RwLock<IpfsCache> = Box::leak(Box::new(RwLock::new(IpfsCache {
            ipfs,
            manifests: HashMap::new(),
            metadata: HashMap::new(),
        })));

        let subgraphs = subgraphs.map(move |subgraphs| async move {
            Ptr::new(Self::subgraphs(&subgraphs, cache, l2_transfer_delay).await)
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
        cache: &'static RwLock<IpfsCache>,
        l2_transfer_delay: Option<chrono::Duration>,
    ) -> HashMap<SubgraphId, Subgraph> {
        join_all(subgraphs.iter().map(|subgraph| async move {
            let deployments = join_all(
                subgraph
                    .versions
                    .iter()
                    .map(|version| Self::deployment(subgraphs, version, cache)),
            )
            .await
            .into_iter()
            .flatten()
            .collect();

            let transferred_to_l2 = matches!(
                (subgraph.started_transfer_to_l2_at, l2_transfer_delay),
                (Some(at), Some(delay)) if Utc::now() - at > delay
            );
            (
                subgraph.id,
                Subgraph {
                    deployments,
                    transferred_to_l2,
                },
            )
        }))
        .await
        .into_iter()
        .collect()
    }

    async fn deployment(
        subgraphs: &[network_subgraph::Subgraph],
        version: &network_subgraph::SubgraphVersion,
        cache: &'static RwLock<IpfsCache>,
    ) -> Option<Arc<Deployment>> {
        let id = version.subgraph_deployment.id;
        let manifest = IpfsCache::manifest(cache, &version.subgraph_deployment.id).await?;
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
                        url: allocation.indexer.url.as_ref()?.parse().ok()?,
                        staked_tokens: allocation.indexer.staked_tokens.change_precision(),
                    },
                })
            })
            .collect();

        let metadata_hash = version
            .metadata_hash
            .as_ref()
            .and_then(|hash| Bytes32::from_str(hash).ok());
        let version = match metadata_hash {
            Some(hash) => IpfsCache::metadata(cache, &hash).await,
            None => None,
        };

        Some(Arc::new(Deployment {
            id,
            manifest,
            version,
            subgraphs,
            allocations,
        }))
    }
}

struct IpfsCache {
    ipfs: Arc<ipfs::Client>,
    manifests: HashMap<DeploymentId, Arc<Manifest>>,
    metadata: HashMap<Bytes32, Arc<semver::Version>>,
}

impl IpfsCache {
    async fn manifest(cache: &RwLock<Self>, deployment: &DeploymentId) -> Option<Arc<Manifest>> {
        let read = cache.read().await;
        if let Some(manifest) = read.manifests.get(deployment) {
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
        write.manifests.insert(*deployment, manifest.clone());
        Some(manifest)
    }

    async fn metadata(cache: &RwLock<Self>, hash: &Bytes32) -> Option<Arc<semver::Version>> {
        let read = cache.read().await;
        if let Some(metadata) = read.metadata.get(hash) {
            return Some(metadata.clone());
        }
        let ipfs = read.ipfs.clone();
        drop(read);

        let metadata = match Self::cat_metadata(&ipfs, hash).await {
            Ok(metadata) => Arc::new(metadata),
            Err(metadata_err) => {
                tracing::warn!(%hash, %metadata_err);
                return None;
            }
        };

        let mut write = cache.write().await;
        write.metadata.insert(*hash, metadata.clone());
        Some(metadata)
    }

    async fn cat_manifest(
        ipfs: &ipfs::Client,
        deployment: &DeploymentId,
    ) -> anyhow::Result<Manifest> {
        // Subgraph manifest schema:
        // https://github.com/graphprotocol/graph-node/blob/master/docs/subgraph-manifest.md
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ManifestSrc {
            data_sources: Vec<DataSource>,
            #[serde(default)]
            features: Vec<String>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct DataSource {
            network: String,
            source: EthereumContractSource,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct EthereumContractSource {
            start_block: Option<u64>,
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

    async fn cat_metadata(ipfs: &ipfs::Client, hash: &Bytes32) -> anyhow::Result<semver::Version> {
        #[derive(Deserialize)]
        struct Metadata {
            label: String,
        }

        // CIDv0 prefix for hex-encoded content address
        let cid = format!("f1220{}", hex::encode(hash.0));

        let payload = ipfs.cat(&cid).await?;
        let metadata: Metadata = serde_json::from_str(&payload)?;
        Ok(metadata.label.trim_start_matches('v').parse()?)
    }
}
