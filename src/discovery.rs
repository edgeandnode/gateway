use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use ipnetwork::IpNetwork;
use reqwest::Url;
use semver::Version;
use thegraph_core::{
    alloy::primitives::{Address, BlockNumber},
    DeploymentId, ProofOfIndexing, SubgraphId,
};
use tokio::{sync::watch, time::MissedTickBehavior};

use crate::{config::BlockedIndexer, errors::UnavailableReason, network};

pub struct Topology {
    pub subgraphs: HashMap<SubgraphId, Subgraph>,
    pub deployments: HashMap<DeploymentId, Arc<Deployment>>,
}

pub struct Subgraph {
    pub id: SubgraphId,
    pub versions: Vec<Arc<Deployment>>,
}

pub struct Deployment {
    pub id: DeploymentId,
    pub chain: String,
    pub start_block: BlockNumber,
    pub allocations: Vec<Allocation>,
}

pub struct Allocation {
    pub id: Address,
    pub indexer: Indexer,
    pub tokens: u128,
    pub status: Result<IndexingStatus, UnavailableReason>,
    pub fee: u128,
}

pub struct Indexer {
    pub id: Address,
    pub url: Url,
    pub staked_tokens: u128,
}

pub struct IndexingStatus {
    pub latest_block: BlockNumber,
    pub min_block: Option<BlockNumber>,
}

pub struct VersionRequirements {
    pub min_indexer_service_version: Version,
    pub min_graph_node_version: Version,
}

pub fn spawn(
    network_subgraph: network::subgraph_client::Client,
    indexer_blocklist: BTreeMap<Address, BlockedIndexer>,
    indexer_host_blocklist: HashSet<IpNetwork>,
    indexer_version_requirements: VersionRequirements,
    poi_blocklist: HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>>,
) -> watch::Receiver<Topology> {
    let (tx, rx) = watch::channel(Topology {
        subgraphs: Default::default(),
        deployments: Default::default(),
    });
    let mut actor = Actor {
        network_subgraph,
        indexer_blocklist,
        indexer_host_blocklist,
        indexer_version_requirements,
        poi_blocklist,
        subgraphs_cache: Default::default(),
    };
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(30));
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            timer.tick().await;
            let topology = actor.update_topology().await;
            tx.send(topology).unwrap();
        }
    });
    rx
}

struct Actor {
    network_subgraph: network::subgraph_client::Client,
    indexer_blocklist: BTreeMap<Address, BlockedIndexer>,
    indexer_host_blocklist: HashSet<IpNetwork>,
    indexer_version_requirements: VersionRequirements,
    poi_blocklist: HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>>,
    subgraphs_cache: Vec<network::subgraph_client::types::Subgraph>,
}

impl Actor {
    async fn update_topology(&mut self) -> Topology {
        match self.network_subgraph.fetch().await {
            Ok(subgraphs) => self.subgraphs_cache = subgraphs,
            Err(network_subgraph_err) => tracing::error!(%network_subgraph_err),
        };

        let mut deployments: Vec<Deployment> = self
            .subgraphs_cache
            .iter()
            .flat_map(|s| {
                s.versions.iter().filter_map(|v| {
                    let manifest = v.subgraph_deployment.manifest.as_ref()?;
                    Some(Deployment {
                        id: v.subgraph_deployment.id,
                        chain: manifest.network.as_ref()?.clone(),
                        start_block: manifest.start_block,
                        allocations: v
                            .subgraph_deployment
                            .allocations
                            .iter()
                            .filter_map(|a| {
                                Some(Allocation {
                                    id: *a.id,
                                    indexer: Indexer {
                                        id: a.indexer.id.0.into(),
                                        url: a.indexer.url.as_ref()?.parse().ok()?,
                                        staked_tokens: a.indexer.staked_tokens,
                                    },
                                    tokens: a.allocated_tokens,
                                    status: Err(UnavailableReason::Internal("unprocessed")),
                                    fee: 0,
                                })
                            })
                            .collect(),
                    })
                })
            })
            .collect();

        // by-indexer:
        todo!("filter indexer URL geoblock");
        todo!("filter indexer-service version");
        todo!("filter graph-node version");
        todo!("filter PoI (longer interval)");
        todo!("indexing statuses");
        todo!("cost models");

        let deployments: HashMap<DeploymentId, Arc<Deployment>> =
            deployments.into_iter().map(|d| (d.id, d.into())).collect();
        let subgraphs: HashMap<SubgraphId, Subgraph> = self
            .subgraphs_cache
            .iter()
            .map(|s| {
                let versions = s
                    .versions
                    .iter()
                    .filter_map(|v| deployments.get(&v.subgraph_deployment.id).cloned())
                    .collect();
                (s.id, Subgraph { id: s.id, versions })
            })
            .collect();
        Topology {
            subgraphs,
            deployments,
        }
    }
}
