//! Ad-hoc implementation of the network resolution service for the Graph Gateway. This service
//! provides information about the subgraphs (and subgraph deployments) registered in the network
//! smart contract, as well as the indexers that are indexing them.

pub use service::{
    NetworkService, NetworkServiceBuilder, NetworkServicePending, ResolvedSubgraphInfo,
};
pub use snapshot::{
    BlockNumber, DeploymentId, Indexer, Indexing, IndexingId, IndexingStatus,
    NetworkTopologySnapshot, SubgraphId,
};

pub mod indexer_addr_blocklist;
pub mod indexer_host_blocklist;
pub mod indexer_host_resolver;
pub mod indexer_indexing_cost_model_compiler;
pub mod indexer_indexing_cost_model_resolver;
pub mod indexer_indexing_poi_blocklist;
pub mod indexer_indexing_poi_resolver;
pub mod indexer_indexing_progress_resolver;
pub mod indexer_version_resolver;
pub mod internal;
mod service;
mod snapshot;
pub mod subgraph;
