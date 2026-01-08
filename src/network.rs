//! Network Topology Management
//!
//! This module provides network topology services for resolving subgraphs and
//! deployments to their available indexers.
//!
//! # Module Structure
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`service`] | Main [`NetworkService`] interface for resolution |
//! | [`subgraph_client`] | Fetches data from network subgraph via trusted indexers |
//! | [`cost_model`] | Resolves indexer query fees |
//! | [`indexing_progress`] | Resolves indexer block progress |
//! | [`host_filter`] | IP-based indexer filtering |
//! | [`version_filter`] | Version-based indexer filtering |
//! | [`poi_filter`] | Proof-of-indexing filtering |
//! | [`indexer_blocklist`] | Manual indexer blocklist |
//!
//! See [`service`] module for architecture details.

pub use service::{NetworkService, ResolvedSubgraphInfo};
pub use snapshot::{DeploymentError, Indexing, IndexingId, SubgraphError};
use thegraph_graphql_http::graphql::{IntoDocument as _, IntoDocumentWithVariables};

pub mod cost_model;
pub mod host_filter;
pub mod indexer_blocklist;
mod indexer_processing;
pub mod indexing_progress;
pub mod poi_filter;
mod pre_processing;
pub mod service;
mod snapshot;
pub mod subgraph_client;
mod subgraph_processing;
pub mod version_filter;

pub struct GraphQlRequest {
    pub document: String,
    pub variables: serde_json::Value,
}
impl IntoDocumentWithVariables for GraphQlRequest {
    type Variables = serde_json::Value;
    fn into_document_with_variables(
        self,
    ) -> (thegraph_graphql_http::graphql::Document, Self::Variables) {
        (self.document.into_document(), self.variables)
    }
}
