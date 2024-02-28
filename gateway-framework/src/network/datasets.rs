use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use alloy_primitives::Address;
use eventuals::{Eventual, Ptr};
use gateway_common::types::GRT;
use thegraph::types::{DeploymentId, SubgraphId};
use toolshed::url::Url;

use super::network_subgraph::Indexer;

/// A dataset (e.g. a subgraph) that has an identifier and zero or more
/// versions/deployments.
#[derive(Clone)]
pub struct Dataset {
    /// Unique identifier on the network.
    pub id: SubgraphId,
    /// Versions of the dataset, ordered oldest to newest.
    pub deployments: Vec<Arc<Deployment>>,
    /// Indicates that the dataset has been transferred to L2.
    pub l2_id: Option<SubgraphId>,
}

/// A concrete deployment of a dataset associated with a manifest.
pub struct Deployment {
    /// Unique identifier on the network.
    pub id: DeploymentId,
    /// Manifest details (like the network indexed).
    pub manifest: Arc<Manifest>,
    /// An indexer may have multiple active allocations on a deployment. We collapse
    /// them into a single logical allocation using the largest allocation ID and
    /// sum of the allocated tokens.
    pub indexers: Vec<Arc<Indexer>>,
    /// A deployment may be associated with multiple datasets.
    pub datasets: BTreeSet<SubgraphId>,
    /// Indicates that the deployment was transfered to L2.
    pub transferred_to_l2: bool,
    /// False if the deployment was denied rewards for some reason.
    pub expect_attestation: bool,
}

/// Details from the deployment manifest.
///
/// This should only include information that is common across
/// all data services.
pub struct Manifest {
    /// The network or chain identifier that the dataset is indexing / deriving data from.
    pub network: String,
    /// The first block that this manifest is processing data from.
    pub min_block: u64,
}

pub struct DeploymentIndexer {
    pub id: Address,
    pub url: Url,
    pub staked_tokens: GRT,
    pub largest_allocation: Address,
    pub allocated_tokens: GRT,
}

/// Representation of the graph network being used to serve queries
#[derive(Clone)]
pub struct Datasets {
    pub datasets: Eventual<Ptr<HashMap<SubgraphId, Dataset>>>,
    pub deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    pub indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
}

impl Datasets {
    pub async fn new() -> Self {}
}
