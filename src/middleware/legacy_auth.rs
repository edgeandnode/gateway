use std::collections::BTreeMap;

use axum::{
    RequestPartsExt,
    body::Body,
    extract::{Path, Request},
    http::{Response, header},
    middleware::Next,
};
use headers::{Authorization, HeaderMapExt};

/// This adapter middleware extracts the authorization token from the `api_key` path parameter,
/// and adds it to the request in the `Authorization` header.
///
/// If the request already has an `Authorization` header, it is left unchanged.
/// If the request does not have an `api_key` path parameter, it is left unchanged.
///
/// This is a temporary adapter middleware to allow legacy clients to use the new auth scheme.
pub async fn legacy_auth_adapter(mut request: Request, next: Next) -> Response<Body> {
    // If the request already has an `Authorization` header, don't do anything
    if !request.headers().contains_key(header::AUTHORIZATION) {
        let (mut parts, body) = request.into_parts();

        // Extract the `api_key` from the path and add it to the Authorization header
        if let Ok(Path(path)) = parts.extract::<Path<BTreeMap<String, String>>>().await
            && let Some(api_key) = path.get("api_key")
        {
            parts
                .headers
                .typed_insert(Authorization::bearer(api_key).expect("valid api_key"));
        }

        // reconstruct the request
        request = Request::from_parts(parts, body);
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{HeaderMap, Method, Request, StatusCode, header::AUTHORIZATION},
        middleware,
        routing::{get, post},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::legacy_auth_adapter;

    fn test_router() -> Router {
        async fn handler(headers: HeaderMap) -> String {
            headers
                .get(AUTHORIZATION)
                .and_then(|header| header.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        }

        let api = Router::new()
            .route("/subgraphs/id/{subgraph_id}", post(handler))
            .route("/{api_key}/subgraphs/id/{subgraph_id}", post(handler))
            .route("/deployments/id/{deployment_id}", post(handler))
            .route("/{api_key}/deployments/id/{deployment_id}", post(handler))
            .layer(middleware::from_fn(legacy_auth_adapter));
        Router::new()
            .route("/", get(|| async { "OK" }))
            .nest("/api", api)
    }

    fn test_body() -> Body {
        Body::from("test")
    }

    #[tokio::test]
    async fn test_preexistent_auth_header() {
        // Given
        let app = test_router();

        let api_key = "deadbeefdeadbeefdeadbeefdeadbeef"; // 32 hex digits
        let auth_header = format!("Bearer {api_key}");

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/subgraphs/id/456")
            .header(AUTHORIZATION, auth_header.clone())
            .body(test_body())
            .unwrap();

        // When
        let res = app.oneshot(request).await.unwrap();

        // Then
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], auth_header.as_bytes());
    }

    #[tokio::test]
    async fn test_legacy_auth() {
        // Given
        let app = test_router();

        let api_key = "deadbeefdeadbeefdeadbeefdeadbeef"; // 32 hex digits
        let auth_header = format!("Bearer {api_key}");

        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/api/{api_key}/subgraphs/id/456"))
            .body(test_body())
            .unwrap();

        // When
        let res = app.oneshot(request).await.unwrap();

        // Then
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], auth_header.as_bytes());
    }

    #[tokio::test]
    async fn test_legacy_auth_with_preexistent_auth_header() {
        // Given
        let app = test_router();

        let api_key = "deadbeefdeadbeefdeadbeefdeadbeef"; // 32 hex digits
        let auth_header = "Bearer 123";

        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/api/{api_key}/subgraphs/id/456"))
            .header(AUTHORIZATION, auth_header)
            .body(test_body())
            .unwrap();

        // When
        let res = app.oneshot(request).await.unwrap();

        // Then
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], auth_header.as_bytes());
    }

    #[tokio::test]
    async fn test_no_auth() {
        // Given
        let app = test_router();

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/subgraphs/id/456")
            .body(test_body())
            .unwrap();

        // When
        let res = app.oneshot(request).await.unwrap();

        // Then
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"");
    }
}
