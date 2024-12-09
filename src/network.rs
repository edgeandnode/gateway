pub use service::{NetworkService, ResolvedSubgraphInfo};
pub use snapshot::{DeploymentError, Indexing, IndexingId, SubgraphError};
use thegraph_graphql_http::graphql::{IntoDocument as _, IntoDocumentWithVariables};

pub mod cost_model;
pub mod host_filter;
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
