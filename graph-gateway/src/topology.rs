use crate::{ipfs, network_subgraph};
use prelude::{eventuals::EventualExt as _, *};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

/// Representation of the graph network being used to serve queries
#[derive(Clone)]
pub struct GraphNetwork {
    pub subgraphs: Eventual<Ptr<HashMap<SubgraphId, Subgraph>>>,
    pub deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    /// Indexer penalty on successful indexing disputes, in parts per million.
    pub slashing_percentage: Eventual<PPM>,
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
    pub async fn new(network_subgraph: network_subgraph::Data, ipfs: Arc<ipfs::Client>) -> Self {
        let (subgraphs_tx, subgraphs_rx) = Eventual::<Ptr<HashMap<SubgraphId, Subgraph>>>::new();
        let (deployments_tx, deployments_rx) =
            Eventual::<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>::new();

        network_subgraph
            .subgraphs
            .pipe_async(move |subgraphs| {
                let ipfs = ipfs.clone();
                async move {
                    Self::assemble_topology(&subgraphs, &ipfs).await;
                }
            })
            .forever();

        // Return only after eventuals have values, to avoid serving client queries prematurely.
        if eventuals::join((subgraphs_rx.clone(), deployments_rx.clone()))
            .value()
            .await
            .is_err()
        {
            panic!("Failed to await Graph network topology");
        }

        Self {
            subgraphs: subgraphs_rx,
            deployments: deployments_rx,
            slashing_percentage: network_subgraph.slashing_percentage,
        }
    }

    async fn assemble_topology(subgraphs: &[network_subgraph::Subgraph], ipfs: &ipfs::Client) {
        todo!();
    }
}
