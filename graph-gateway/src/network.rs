//! Ad-hoc implementation of the network resolution service for the Graph Gateway. This service
//! provides information about the subgraphs (and subgraph deployments) registered in the network
//! smart contract, as well as the indexers that are indexing them.

pub use entities::{
    BlockNumber, DeploymentId, GraphNetwork, Indexer, Indexing, IndexingId, IndexingStatus,
    SubgraphId,
};
pub use service::{
    NetworkService, NetworkServiceBuilder, NetworkServicePending, ResolvedSubgraphInfo,
};

mod entities;
pub mod indexers_addr_blocklist;
pub mod indexers_cost_model_compiler;
pub mod indexers_cost_model_resolver;
pub mod indexers_host_blocklist;
pub mod indexers_host_resolver;
pub mod indexers_indexing_status_resolver;
pub mod indexers_poi_blocklist;
pub mod indexers_poi_resolver;
pub mod internal;
mod service;
pub mod subgraph;
