//! Ad-hoc implementation of the network resolution service for the Graph Gateway. This service
//! provides information about the subgraphs (and subgraph deployments) registered in the network
//! smart contract, as well as the indexers that are indexing them.

pub use entities::*;
pub use service::*;

pub mod blocklists;
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
pub mod ttl_hash_map;
