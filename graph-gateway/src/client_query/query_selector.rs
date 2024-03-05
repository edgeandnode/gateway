use std::collections::HashMap;

use anyhow::anyhow;
use axum::async_trait;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum::response::IntoResponse;
use thegraph_core::types::{DeploymentId, SubgraphId};

use gateway_framework::errors::Error;
use gateway_framework::graphql;

/// Rejection type for the query selector extractor, [`QuerySelector`].
///
/// This is a thin wrapper around [`Error`] and implements [`IntoResponse`] to return a GraphQL
/// error response.
#[derive(Debug)]
pub struct QuerySelectorRejection(Error);

impl From<Error> for QuerySelectorRejection {
    fn from(value: Error) -> Self {
        Self(value)
    }
}

impl IntoResponse for QuerySelectorRejection {
    fn into_response(self) -> axum::response::Response {
        graphql::error_response(self.0).into_response()
    }
}

/// Extractor for the GraphQL query selector, i.e. a `DeploymentId` or `SubgraphId`.
///
/// If the path parameter parsing fails, a GraphQL error response is returned indicating that
/// the provided ID is invalid.
#[derive(Debug, Clone)]
pub enum QuerySelector {
    /// The query selector is a [`DeploymentId`].
    Deployment(DeploymentId),
    /// The query selector is a [`SubgraphId`].
    Subgraph(SubgraphId),
}

impl std::fmt::Display for QuerySelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuerySelector::Deployment(id) => write!(f, "{}", id),
            QuerySelector::Subgraph(id) => write!(f, "{}", id),
        }
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for QuerySelector
where
    S: Send + Sync,
{
    type Rejection = QuerySelectorRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // Get the path parameters
        let Path(params) = Path::<HashMap<String, String>>::from_request_parts(parts, state)
            .await
            .map_err(|_| Error::SubgraphNotFound(anyhow!("invalid id")))?;

        // Get the query selector from the path parameters and parse it
        let selector = if let Some(param) = params.get("subgraph_id") {
            // Parse the Subgraph ID
            let subgraph_id = param
                .parse()
                .map_err(|_| Error::SubgraphNotFound(anyhow!("invalid subgraph ID: {param}")))?;
            Self::Subgraph(subgraph_id)
        } else if let Some(param) = params.get("deployment_id") {
            // Parse the Deployment ID
            let deployment_id = param
                .parse()
                .map_err(|_| Error::SubgraphNotFound(anyhow!("invalid deployment ID: {param}")))?;
            Self::Deployment(deployment_id)
        } else {
            return Err(Error::SubgraphNotFound(anyhow!("missing identifier")).into());
        };

        // Set the span selector attribute
        tracing::span::Span::current().record("selector", tracing::field::display(&selector));

        Ok(selector)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use axum::body::{Body, BoxBody};
    use axum::http::{Method, Request};
    use axum::Router;
    use thegraph_core::types::{DeploymentId, SubgraphId};
    use tower::ServiceExt;

    use super::QuerySelector;

    /// Create a test router.
    fn test_router() -> Router {
        async fn handle_query(selector: QuerySelector) -> String {
            format!("{}", selector)
        }

        Router::new()
            .route(
                "/deployments/id/:deployment_id",
                axum::routing::post(handle_query),
            )
            .route(
                "/subgraphs/id/:subgraph_id",
                axum::routing::post(handle_query),
            )
    }

    /// Test utility function to create a valid `DeploymentId` with an arbitrary deployment id/ipfs hash.
    fn test_deployment_id(deployment: &str) -> DeploymentId {
        deployment.parse().expect("invalid deployment id/ipfs hash")
    }

    /// Test utility function to create a valid `SubgraphId` with an arbitrary address.
    fn test_subgraph_id(address: &str) -> SubgraphId {
        address.parse().expect("invalid subgraph id")
    }

    /// Deserialize a GraphQL response body.
    async fn deserialize_graphql_response_body<T>(
        body: &mut BoxBody,
    ) -> serde_json::Result<thegraph_graphql_http::http::response::ResponseBody<T>>
    where
        for<'de> T: serde::Deserialize<'de>,
    {
        let body = hyper::body::to_bytes(body).await.expect("valid body");
        serde_json::from_slice(body.as_ref())
    }

    /// Parse text response body.
    async fn parse_text_response_body(body: &mut BoxBody) -> anyhow::Result<String> {
        let body = hyper::body::to_bytes(body).await.expect("valid body");
        let text = String::from_utf8(body.to_vec())?;
        Ok(text)
    }

    #[tokio::test]
    async fn valid_deployment_id() {
        //* Given
        let app = test_router();

        let deployment_id = test_deployment_id("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH");

        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/deployments/id/{deployment_id}"))
            .body(Body::empty())
            .unwrap();

        //* When
        let mut res = app.oneshot(req).await.expect("valid request");

        //* Then
        assert_matches!(parse_text_response_body(res.body_mut()).await, Ok(res_body) => {
            assert_eq!(res_body, deployment_id.to_string());
        });
    }

    #[tokio::test]
    async fn invalid_deployment_id() {
        //* Given
        let app = test_router();

        let deployment_id = "test-invalid-deployment-id";

        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/deployments/id/{deployment_id}"))
            .body(Body::empty())
            .unwrap();

        //* When
        let mut res = app.oneshot(req).await.expect("valid request");

        //* Then
        assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
            assert_eq!(res_body.errors.len(), 1);
            assert_eq!(res_body.errors[0].message, r#"subgraph not found: invalid deployment ID: test-invalid-deployment-id"#);
        });
    }

    #[tokio::test]
    async fn valid_subgraph_id() {
        //* Given
        let app = test_router();

        let subgraph_id = test_subgraph_id("184ba627DB853244c9f17f3Cb4378cB8B39bf147");

        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/subgraphs/id/{subgraph_id}"))
            .body(Body::empty())
            .unwrap();

        //* When
        let mut res = app.oneshot(req).await.expect("valid request");

        //* Then
        assert_matches!(parse_text_response_body(res.body_mut()).await, Ok(res_body) => {
            assert_eq!(res_body, subgraph_id.to_string());
        });
    }

    #[tokio::test]
    async fn invalid_subgraph_id() {
        //* Given
        let app = test_router();

        let subgraph_id = "test-invalid-subgraph-id";

        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/subgraphs/id/{subgraph_id}"))
            .body(Body::empty())
            .unwrap();

        //* When
        let mut res = app.oneshot(req).await.expect("valid request");

        //* Then
        assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
            assert_eq!(res_body.errors.len(), 1);
            assert_eq!(res_body.errors[0].message, "subgraph not found: invalid subgraph ID: test-invalid-subgraph-id");
        });
    }
}
