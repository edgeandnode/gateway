//! GraphQL Response Utilities
//!
//! Helpers for creating GraphQL-compliant error responses.
//!
//! # Response Format
//!
//! All errors are returned as HTTP 200 with GraphQL error body:
//!
//! ```json
//! {
//!   "data": null,
//!   "errors": [{ "message": "error message here" }]
//! }
//! ```
//!
//! This follows the GraphQL spec where transport errors (HTTP 4xx/5xx) are
//! reserved for network issues, while query errors are returned as GraphQL errors.

use axum::http::{Response, StatusCode};
use headers::ContentType;
use thegraph_graphql_http::http::response::{IntoError as IntoGraphqlResponseError, ResponseBody};
use thegraph_headers::HttpBuilderExt as _;

/// Serialize an error into a GraphQL error response.
///
/// This helper function serializes an error into a GraphQL error response JSON string.
fn error_response_body(message: impl IntoGraphqlResponseError) -> String {
    let response_body: ResponseBody<()> = ResponseBody::from_error(message);
    serde_json::to_string(&response_body).expect("failed to serialize error response")
}

/// Create a GraphQL error response.
pub fn error_response(err: impl IntoGraphqlResponseError) -> Response<String> {
    Response::builder()
        .status(StatusCode::OK)
        .header_typed(ContentType::json())
        .body(error_response_body(err))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use axum::http::StatusCode;
    use headers::{ContentType, HeaderMapExt};
    use thegraph_graphql_http::http::response::ResponseBody;

    use super::error_response;

    /// Deserialize a GraphQL response body.
    fn deserialize_response_body(body: &str) -> serde_json::Result<ResponseBody<()>> {
        serde_json::from_str(body)
    }

    /// A test error type implementing `std::error::Error` trait.
    ///
    /// See [`create_graphql_error_response`] for more details
    #[derive(Debug, thiserror::Error)]
    #[error("test error: {cause}")]
    struct TestError {
        cause: String,
    }

    /// Ensure that the error response body is correctly serialized.
    #[test]
    fn create_graphql_error_response() {
        //* Given
        let error = TestError {
            cause: "test message".to_string(),
        };

        //* When
        let response = error_response(error);

        //* Then
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().typed_get(), Some(ContentType::json()));

        // Ensure that the response body is a valid GraphQL error response.
        assert_matches!(deserialize_response_body(response.body()), Ok(resp_body) => {
            // No data should be returned
            assert_eq!(resp_body.data, None);
            // There should be one error
            assert_eq!(resp_body.errors.len(), 1);
            assert_eq!(resp_body.errors[0].message, "test error: test message");
        });
    }
}
