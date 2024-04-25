//! Ad-hoc implementation of the network resolution service for the Graph Gateway. This service
//! provides information about the subgraphs (and subgraph deployments) registered in the network
//! smart contract, as well as the indexers that are indexing them.

pub mod blocklists;
pub mod indexers_addr_blocklist;
pub mod indexers_host_blocklist;
pub mod indexers_host_resolver;
pub mod subgraph;
