pub use errors::{DeploymentError, SubgraphError};
pub use service::{NetworkService, ResolvedSubgraphInfo};
pub use snapshot::{Indexing, IndexingId};
use thegraph_graphql_http::graphql::{IntoDocument as _, IntoDocumentWithVariables};

pub mod cost_model;
mod errors;
pub mod host_filter;
pub mod indexer_indexing_poi_blocklist;
pub mod indexer_indexing_poi_resolver;
mod indexer_processing;
pub mod indexing_progress;
mod pre_processing;
pub mod service;
mod snapshot;
pub mod subgraph_client;
mod subgraph_processing;
pub mod version_filter;

pub struct GraphQlRequest {
    document: String,
    variables: serde_json::Value,
}
impl IntoDocumentWithVariables for GraphQlRequest {
    type Variables = serde_json::Value;
    fn into_document_with_variables(
        self,
    ) -> (thegraph_graphql_http::graphql::Document, Self::Variables) {
        (self.document.into_document(), self.variables)
    }
}
