use serde::Deserialize;
use thegraph_core::types::DeploymentId;
use thegraph_graphql_http::{
    graphql::{Document, IntoDocument, IntoDocumentWithVariables},
    http_client::{RequestError, ReqwestExt, ResponseError},
};

use super::urls::CostUrl;

const COST_MODEL_QUERY_DOCUMENT: &str = indoc::indoc! {r#"
    query costModels($deployments: [String!]!) {
        costModels(deployments: $deployments) {
            deployment
            model
            variables
        }
    }
"#};

/// Errors that can occur while fetching the cost models.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// The request failed.
    #[error("request error: {0}")]
    RequestError(String),

    /// Invalid response.
    ///
    /// The response could not be deserialized or is missing required fields.
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// The response did not contain any cost model information.
    #[error("empty response")]
    EmptyResponse,
}

/// Send a request to the indexer to get the cost models of the given deployments.
pub async fn send_request(
    client: &reqwest::Client,
    url: CostUrl,
    deployments: impl IntoIterator<Item = &DeploymentId>,
) -> Result<Vec<CostModelSource>, Error> {
    let resp = client
        .post(url.into_inner())
        .send_graphql::<Response>(Request::new(deployments))
        .await
        .map_err(|err| match err {
            RequestError::RequestSerializationError(..) => {
                unreachable!("request serialization should not fail")
            }
            RequestError::RequestSendError(..) | RequestError::ResponseRecvError(..) => {
                Error::RequestError(err.to_string())
            }
            RequestError::ResponseDeserializationError { .. } => {
                Error::InvalidResponse(err.to_string())
            }
        })?
        .map_err(|err| match err {
            ResponseError::Failure { .. } => Error::RequestError(err.to_string()),
            ResponseError::Empty => Error::EmptyResponse,
        })?;

    Ok(resp.cost_models)
}

/// The request type for the cost model query.
///
/// This is a GraphQL query that fetches the cost models for a set of deployments.
///
/// See [`COST_MODEL_QUERY_DOCUMENT`] for the query document.
#[derive(Debug, Clone)]
struct Request {
    document: Document,
    vars_deployments: Vec<String>,
}

impl Request {
    /// Create a new cost model query request.
    pub fn new<'a>(deployments: impl IntoIterator<Item = &'a DeploymentId>) -> Self {
        let deployments = deployments
            .into_iter()
            .map(|item| item.to_string())
            .collect();
        Self {
            document: COST_MODEL_QUERY_DOCUMENT.into_document(),
            vars_deployments: deployments,
        }
    }
}

impl IntoDocumentWithVariables for Request {
    type Variables = serde_json::Value;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (
            self.document,
            serde_json::json!({ "deployments": self.vars_deployments }),
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    cost_models: Vec<CostModelSource>,
}

#[derive(Debug, Deserialize)]
pub struct CostModelSource {
    pub deployment: DeploymentId,
    pub model: String,
    pub variables: Option<String>,
}
