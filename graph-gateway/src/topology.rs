use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::anyhow;
use eventuals::{Eventual, EventualExt, Ptr};
use futures::future::join_all;
use indoc::indoc;
use itertools::Itertools;
use prelude::GRT;
use serde::Deserialize;
use thegraph::client as subgraph_client;
use thegraph::types::{DeploymentId, SubgraphId};
use tokio::sync::RwLock;
use toolshed::url::Url;

use crate::{config, ipfs, network_subgraph, spawn_poller};

/// Representation of the graph network being used to serve queries
#[derive(Clone)]
pub struct GraphNetwork {
    pub subgraphs: Eventual<Ptr<HashMap<SubgraphId, Subgraph>>>,
    pub deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    pub indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
}

/// In an effort to keep the ownership structure a simple tree, this only contains the info required
/// to resolve queries by `SubgraphId` into the relevant deployments. Therefore, there is no need
/// for a query by `DeploymentId` to interact with this.
#[derive(Clone)]
pub struct Subgraph {
    /// Subgraph versions, in ascending order
    pub deployments: Vec<Arc<Deployment>>,
    pub id: SubgraphId,
    /// Indicates that the subgraph has been transferred to L2, and should not be served directly by
    /// this gateway.
    pub l2_id: Option<SubgraphId>,
}

pub struct Deployment {
    pub id: DeploymentId,
    pub manifest: Arc<Manifest>,
    pub expect_attestation: bool,
    /// An indexer may have multiple active allocations on a deployment. We collapse them into a single logical
    /// allocation using the largest allocation ID and sum of the allocated tokens.
    pub indexers: Vec<Arc<Indexer>>,
    /// A deployment may be associated with multiple subgraphs.
    pub subgraphs: BTreeSet<SubgraphId>,
    /// Indicates that the deployment should not be served directly by this gateway. This will
    /// always be false when `allocations > 0`.
    pub transferred_to_l2: bool,
}

pub struct Allocation {
    pub id: Address,
    pub allocated_tokens: GRT,
    pub indexer: Arc<Indexer>,
}

pub struct Indexer {
    pub id: Address,
    pub url: Url,
    pub staked_tokens: GRT,
    pub largest_allocation: Address,
    pub allocated_tokens: GRT,
}

impl Indexer {
    pub fn cost_url(&self) -> Url {
        // Indexer URLs are validated when they are added to the network, so this should never fail.
        // 7f2f89aa-24c9-460b-ab1e-fc94697c4f4
        self.url.join("cost").unwrap().into()
    }

    pub fn status_url(&self) -> Url {
        // Indexer URLs are validated when they are added to the network, so this should never fail.
        // 7f2f89aa-24c9-460b-ab1e-fc94697c4f4
        self.url.join("status").unwrap().into()
    }
}

pub struct Manifest {
    pub network: String,
    pub features: Vec<String>,
    pub min_block: u64,
}

impl GraphNetwork {
    pub async fn new(
        subgraphs: Eventual<Ptr<Vec<network_subgraph::Subgraph>>>,
        block_oracle_chains: Eventual<Ptr<HashSet<String>>>,
        ipfs: Arc<ipfs::Client>,
    ) -> Self {
        let cache: &'static RwLock<IpfsCache> = Box::leak(Box::new(RwLock::new(IpfsCache {
            ipfs,
            manifests: HashMap::new(),
        })));

        // Create a lookup table for subgraphs, keyed by their ID.
        // Invalid URL indexers are filtered out. See: 7f2f89aa-24c9-460b-ab1e-fc94697c4f4
        let subgraphs = subgraphs.map(move |subgraphs| {
            let block_oracle_chains = block_oracle_chains.clone();
            async move { Ptr::new(Self::subgraphs(&subgraphs, cache, block_oracle_chains).await) }
        });

        // Create a lookup table for deployments, keyed by their ID (which is also their IPFS hash).
        let deployments = subgraphs.clone().map(|subgraphs| async move {
            subgraphs
                .values()
                .flat_map(|subgraph| &subgraph.deployments)
                .map(|deployment| (deployment.id, deployment.clone()))
                .collect::<HashMap<DeploymentId, Arc<Deployment>>>()
                .into()
        });

        // Create a lookup table for indexers, keyed by their ID (which is also their address).
        let indexers = subgraphs.clone().map(|subgraphs| async move {
            subgraphs
                .values()
                .flat_map(|subgraph| &subgraph.deployments)
                .flat_map(|deployment| &deployment.indexers)
                .map(|indexer| (indexer.id, indexer.clone()))
                .collect::<HashMap<Address, Arc<Indexer>>>()
                .into()
        });

        // Return only after eventuals have values, to avoid serving client queries prematurely.
        if deployments.value().await.is_err() || indexers.value().await.is_err() {
            panic!("Failed to await Graph network topology");
        }

        Self {
            subgraphs,
            deployments,
            indexers,
        }
    }

    async fn subgraphs(
        subgraphs: &[network_subgraph::Subgraph],
        cache: &'static RwLock<IpfsCache>,
        block_oracle_chains: Eventual<Ptr<HashSet<String>>>,
    ) -> HashMap<SubgraphId, Subgraph> {
        let block_oracle_chains: &HashSet<String> =
            &block_oracle_chains.value_immediate().unwrap_or_default();
        join_all(subgraphs.iter().map(|subgraph| async move {
            let id = subgraph.id;
            let deployments =
                join_all(subgraph.versions.iter().map(|version| {
                    Self::deployment(subgraphs, version, cache, block_oracle_chains)
                }))
                .await
                .into_iter()
                .flatten()
                .collect();
            let subgraph = Subgraph {
                deployments,
                id,
                l2_id: subgraph.id_on_l2,
            };
            (id, subgraph)
        }))
        .await
        .into_iter()
        .collect()
    }

    async fn deployment(
        subgraphs: &[network_subgraph::Subgraph],
        version: &network_subgraph::SubgraphVersion,
        cache: &'static RwLock<IpfsCache>,
        block_oracle_chains: &HashSet<String>,
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

        // extract indexer info from each allocation
        let indexers = version
            .subgraph_deployment
            .allocations
            .iter()
            .filter_map(|allocation| {
                // If indexer URL parsing fails, the allocation is ignored (filtered out).
                // 7f2f89aa-24c9-460b-ab1e-fc94697c4f4
                let url = allocation.indexer.url.as_ref()?.parse().ok()?;

                let id = allocation.indexer.id;
                Some((
                    id,
                    Indexer {
                        id,
                        url,
                        staked_tokens: GRT(allocation.indexer.staked_tokens.into()),
                        largest_allocation: allocation.id,
                        allocated_tokens: GRT(allocation.allocated_tokens.into()),
                    },
                ))
            })
            .into_group_map() // TODO: remove need for itertools here: https://github.com/rust-lang/rust/issues/80552
            .into_iter()
            .filter_map(|(_, mut allocations)| {
                let total_allocation = GRT(allocations.iter().map(|a| a.allocated_tokens.0).sum());
                // last allocation is latest: 9936786a-e286-45f3-9190-8409d8389e88
                let mut indexer = allocations.pop()?;
                indexer.allocated_tokens = total_allocation;
                Some(indexer)
            })
            .map(Arc::new)
            .collect();

        // abf62a6d-c071-4507-b528-ddc8e250127a
        let transferred_to_l2 = version.subgraph_deployment.transferred_to_l2
            && version.subgraph_deployment.allocations.is_empty();

        let expect_attestation =
            manifest.features.is_empty() && block_oracle_chains.contains(&manifest.network);

        Some(Arc::new(Deployment {
            id,
            manifest,
            expect_attestation,
            subgraphs,
            indexers,
            transferred_to_l2,
        }))
    }
}

struct IpfsCache {
    ipfs: Arc<ipfs::Client>,
    manifests: HashMap<DeploymentId, Arc<Manifest>>,
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

        let payload = ipfs.cat(&deployment.to_string()).await?;
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

/// Returns the set of CAIP-2 IDs supported by the Block Oracle.
pub async fn block_oracle_chains(
    subgraph: subgraph_client::Client,
    chains: &[config::Chain],
) -> Eventual<Ptr<HashSet<String>>> {
    let query = indoc! {"
        networks(
            block: $block
            orderBy: id
            orderDirection: asc
            first: $first
            where: {
                id_gt: $last
            }
        ) {
            id
        }
    "};
    #[derive(Clone, Deserialize)]
    struct Network {
        id: String,
    }
    let chains: &'static HashMap<String, HashSet<String>> = Box::leak(Box::new(
        chains
            .iter()
            .map(|c| (c.caip2_id.to_string(), HashSet::from_iter(c.names.clone())))
            .collect::<HashMap<_, _>>(),
    ));
    let reader = spawn_poller::<Network>(
        subgraph,
        query.into(),
        "block_oracle",
        Duration::from_secs(120),
    )
    .map(move |networks| async move {
        networks
            .iter()
            .flat_map(|n| chains.get(&n.id).cloned().unwrap_or_default())
            .collect::<HashSet<String>>()
            .into()
    });
    reader.value().await.unwrap();
    reader
}
