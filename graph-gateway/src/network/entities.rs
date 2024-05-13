//! Entities that are used to represent the network topology.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    ops::Deref,
    sync::Arc,
};

pub use alloy_primitives::{Address, BlockNumber};
use cost_model::CostModel;
use custom_debug::CustomDebug;
use eventuals::Ptr;
use semver::Version;
pub use thegraph_core::types::{DeploymentId, SubgraphId};
use url::Url;

/// The [`IndexingId`] struct represents the unique identifier of an indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingId {
    /// The indexer's ID indexing the subgraph's deployment.
    pub indexer: Address,
    /// The subgraph's deployment ID indexed by the indexer.
    pub deployment: DeploymentId,
}

#[derive(Clone)]
pub struct Indexing {
    /// The indexing unique identifier.
    pub id: IndexingId,

    /// The versions behind the highest version of the subgraph being indexed.
    pub versions_behind: u8,

    /// The largest allocation address.
    ///
    /// This is, among all allocations associated with the indexer and deployment, the address
    /// with the largest amount of allocated tokens.
    pub largest_allocation: Address,
    /// The indexer's indexing total allocated tokens.
    ///
    /// This is, the sum of all allocated tokens associated with the indexer and deployment.
    pub total_allocated_tokens: u128,

    /// The indexer
    pub indexer: Arc<Indexer>,

    /// The indexer's indexing status
    pub status: Option<IndexingStatus>,
    /// The indexer's indexing cost model
    pub cost_model: Option<Ptr<CostModel>>,
}

/// The [`IndexingStatus`] struct represents the indexer's indexing status.
#[derive(Debug, Clone)]
pub struct IndexingStatus {
    /// The latest block the indexer has indexed for the deployment.
    pub latest_block: BlockNumber,
    /// The minimum block the indexer has indexed for the deployment.
    pub min_block: Option<BlockNumber>,
}

/// The [`Indexer`] struct represents an indexer in the network topology.
///
/// The indexer is a network  node that indexes a subgraph using one of its versions, i.e., one of
/// the subgraph's deployments. The [`Indexing`] struct represents the indexer's indexing of a
/// subgraph's deployment.
#[derive(CustomDebug, Clone)]
pub struct Indexer {
    /// The indexer's ID.
    pub id: Address,

    /// The indexer's URL.
    ///
    /// It is guaranteed that the URL scheme is either HTTP or HTTPS and the URL has a host.
    #[debug(with = Display::fmt)]
    pub url: Url,

    /// The indexer's "indexer service" version.
    pub indexer_agent_version: Version,
    /// The indexer's "graph node" version.
    pub graph_node_version: Version,

    /// Whether the indexer uses the legacy scalar.
    pub legacy_scalar: bool,

    /// The indexer's indexings set.
    ///
    /// It is a set of deployment IDs that the indexer is indexing.
    pub indexings: HashSet<DeploymentId>,
}

#[derive(Clone)]
pub struct Subgraph {
    /// Subgraph ID
    pub id: SubgraphId,

    /// The Subgraph ID on L2.
    ///
    /// It indicates that the subgraph has been transferred to L2, in that case it should not be
    /// should not be served directly by this gateway.
    pub l2_id: Option<SubgraphId>,
    /// Whether the subgraph has been transferred to L2.
    ///
    /// If all associated deployments have been transferred to L2, it should not be served directly
    /// by this gateway.
    pub transferred_to_l2: bool,

    /// The subgraph's chain name.
    ///
    /// This information is extracted from the highest version of the subgraph deployment's
    /// manifest.
    pub chain: String,
    /// The subgraph's start block number.
    ///
    /// This information is extracted from the highest version of the subgraph deployment's
    /// manifest.
    pub start_block: BlockNumber,

    /// The subgraph's deployments.
    ///
    /// A list of deployment IDs known to be healthy and currently serving queries.
    pub deployments: HashSet<DeploymentId>,

    /// The subgraph's indexings.
    ///
    /// A table holding all the known healthy indexings for the subgraph.
    pub indexings: HashMap<IndexingId, Indexing>,
}

#[derive(Clone)]
pub struct Deployment {
    /// Deployment ID.
    ///
    /// The IPFS content ID of the subgraph manifest.
    pub id: DeploymentId,

    /// Whether the deployment has been transferred to L2.
    ///
    /// If the deployment has been transferred to L2, it should not be served directly by this
    /// gateway.
    /// NOTE: This will always be false when `allocations > 0`.
    pub transferred_to_l2: bool,

    /// The deployment chain name.
    ///
    /// This field is extracted from the deployment manifest.
    pub chain: String,
    /// The deployment start block number.
    ///
    /// This field is extracted from the deployment manifest.
    pub start_block: BlockNumber,

    /// A deployment may be associated with multiple subgraphs.
    pub subgraphs: HashSet<SubgraphId>,

    /// The deployment's indexings.
    ///
    /// A table holding all the known healthy indexings for the deployment.
    pub indexings: HashMap<IndexingId, Indexing>,
}

/// The [`GraphNetwork`] struct represents a snapshot of the network topology.
pub struct GraphNetwork {
    pub(super) subgraphs: HashMap<SubgraphId, Arc<Subgraph>>,
    pub(super) deployments: HashMap<DeploymentId, Arc<Deployment>>,
}

impl GraphNetwork {
    /// Get the [`Subgraph`] by [`SubgraphId`].
    ///
    /// If the subgraph is not found, it returns `None`.
    pub fn get_subgraph_by_id(&self, id: &SubgraphId) -> Option<Arc<Subgraph>> {
        self.subgraphs.get(id).cloned()
    }

    /// Get the [`Deployment`] by [`DeploymentId`].
    ///
    /// If the deployment is not found, it returns `None`.
    pub fn get_deployment_by_id(&self, id: &DeploymentId) -> Option<Arc<Deployment>> {
        self.deployments.get(id).cloned()
    }

    /// Get the snapshot subgraphs.
    pub fn subgraphs(&self) -> impl Deref<Target = HashMap<SubgraphId, Arc<Subgraph>>> + '_ {
        &self.subgraphs
    }

    /// Get the snapshot deployments.
    pub fn deployments(&self) -> impl Deref<Target = HashMap<DeploymentId, Arc<Deployment>>> + '_ {
        &self.deployments
    }
}
