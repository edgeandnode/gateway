//! Authorization middleware.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::http::Request;
use headers::authorization::Bearer;
use headers::{Authorization, HeaderMapExt, Origin};
use tower::Service;

use gateway_framework::errors::Error;
use gateway_framework::graphql;

use crate::reports;

use super::auth::{AuthContext, AuthToken};

#[pin_project::pin_project(project = KindProj)]
enum Kind<F> {
    InnerService(#[pin] F),
    Error {
        response: Option<axum::response::Response>,
    },
}

/// Response future for [`RequireAuthorization`].
#[pin_project::pin_project]
pub struct ResponseFuture<F, R> {
    #[pin]
    kind: Kind<F>,
    _resp: std::marker::PhantomData<R>,
}

impl<F, R, E> Future for ResponseFuture<F, R>
where
    R: axum::response::IntoResponse,
    F: Future<Output = Result<axum::response::Response, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::InnerService(future) => future.poll(cx).map(Into::into),
            KindProj::Error { response } => {
                let response = response.take().expect("future polled after completion");
                Poll::Ready(Ok(response))
            }
        }
    }
}

impl<F, R> ResponseFuture<F, R> {
    /// Create a new [`ResponseFuture`] from a future.
    fn from_service(future: F) -> Self {
        Self {
            kind: Kind::InnerService(future),
            _resp: std::marker::PhantomData,
        }
    }

    /// Create a new [`ResponseFuture`] from an error response.
    fn error(response: impl axum::response::IntoResponse) -> Self {
        Self {
            kind: Kind::Error {
                response: Some(response.into_response()),
            },
            _resp: std::marker::PhantomData,
        }
    }
}

/// Middleware that authorizes requests based on the `Authorization` header.
///
/// The request is not authorized if the `Authorization` header is not present or the bearer
/// token is invalid, in this the middleware returns a GraphQL error response.
///
/// Otherwise, the middleware forwards the request to the inner service inserting an `AuthToken`
/// extension into the request.
///
/// If the `AuthToken` extension is already present, the middleware passes the request to the inner
/// service without doing anything.
#[derive(Clone)]
pub struct RequireAuthorization<S> {
    inner: S,
    ctx: AuthContext,
}

impl<S> RequireAuthorization<S> {
    /// Create a new [`RequireAuthorization`] middleware.
    pub fn new(inner: S, ctx: AuthContext) -> Self {
        Self { inner, ctx }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for RequireAuthorization<S>
where
    S: Service<Request<ReqBody>, Response = axum::response::Response>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future, axum::response::Response>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        // If extension is already present, do nothing
        if req.extensions().get::<AuthToken>().is_some() {
            return ResponseFuture::from_service(self.inner.call(req));
        }

        let bearer = match req.headers().typed_try_get::<Authorization<Bearer>>() {
            Ok(Some(Authorization(bearer))) => bearer,
            Ok(None) => {
                // If the `Authorization` header is not present, return an error response
                return ResponseFuture::error(graphql::error_response(Error::Auth(
                    anyhow::anyhow!("Missing Authorization header"),
                )));
            }
            Err(_) => {
                // If the `Authorization` header is invalid, return an error response
                return ResponseFuture::error(graphql::error_response(Error::Auth(
                    anyhow::anyhow!("Invalid Authorization header"),
                )));
            }
        };

        // Parse the bearer token into an `AuthToken`
        let (auth_token, query_settings, rate_limit_settings) =
            match self.ctx.parse_auth_token(bearer.token()) {
                Ok(token) => token,
                Err(err) => {
                    // If the bearer token is invalid, return an error response
                    return ResponseFuture::error(graphql::error_response(Error::Auth(
                        anyhow::anyhow!("Invalid bearer token: {err}"),
                    )));
                }
            };

        match &auth_token {
            AuthToken::StudioApiKey(api_key) => tracing::info!(
                target: reports::CLIENT_QUERY_TARGET,
                user_address = ?api_key.user_address,
                api_key = %api_key.key,
            ),
            AuthToken::SubscriptionsAuthToken(claims) => tracing::info!(
                target: reports::CLIENT_QUERY_TARGET,
                user_address = ?claims.user(),
            ),
        };

        // Check if the request origin domain is authorized
        let origin = req.headers().typed_get::<Origin>().unwrap_or(Origin::NULL);
        tracing::debug!(domain = %origin.hostname());

        if !auth_token.is_domain_authorized(origin.hostname()) {
            // If the request origin domain is not allowed, return an error response
            return ResponseFuture::error(graphql::error_response(Error::Auth(anyhow::anyhow!(
                "Domain not authorized by user"
            ))));
        }

        // Insert the `AuthToken` extension into the request
        req.extensions_mut().insert(auth_token);

        // Insert the `RateLimitSettings` extension into the request
        if let Some(rate_limit_settings) = rate_limit_settings {
            req.extensions_mut().insert(rate_limit_settings);
        }

        // Insert the `QuerySettings` extension into the request
        if let Some(query_settings) = query_settings {
            req.extensions_mut().insert(query_settings);
        }

        ResponseFuture::from_service(self.inner.call(req))
    }
}

/// A layer that applies [`RequireAuthorization`] which requires the requests to be authorized.
///
/// See [`RequireAuthorization`] for more details.
#[derive(Clone)]
pub struct RequireAuthorizationLayer {
    ctx: AuthContext,
}

impl RequireAuthorizationLayer {
    /// Create a new [`RequireAuthorizationLayer`] middleware.
    pub fn new(ctx: AuthContext) -> Self {
        Self { ctx }
    }
}

impl<S> tower::layer::Layer<S> for RequireAuthorizationLayer {
    type Service = RequireAuthorization<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequireAuthorization::new(inner, self.ctx.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use axum::body::BoxBody;
    use eventuals::{Eventual, Ptr};
    use headers::{Authorization, ContentType, HeaderMapExt};
    use hyper::http;
    use tokio_test::assert_ready_ok;

    use indexer_selection::NotNan;

    use crate::client_query::query_settings::QuerySettings;
    use crate::subgraph_studio::APIKey;

    use super::{AuthContext, AuthToken, RequireAuthorizationLayer};

    fn test_auth_ctx(key: Option<&str>) -> AuthContext {
        let mut ctx = AuthContext {
            api_keys: Eventual::from_value(Ptr::new(Default::default())),
            special_api_keys: Default::default(),
            special_query_key_signers: Default::default(),
            api_key_payment_required: false,
            subscriptions: Eventual::from_value(Ptr::new(Default::default())),
            subscription_rate_per_query: 0,
            subscription_domains: Default::default(),
            subscription_query_counters: Default::default(),
        };
        if let Some(key) = key {
            ctx.api_keys = Eventual::from_value(Ptr::new(HashMap::from([(
                key.into(),
                Arc::new(APIKey {
                    key: key.into(),
                    max_budget_usd: Some(NotNan::new(1e3).unwrap()),
                    ..Default::default()
                }),
            )])));
        }
        ctx
    }

    /// Create a test request without an `Authorization` header or `AuthToken` extension.
    fn test_req_unauthenticated() -> http::Request<()> {
        http::Request::builder().body(()).unwrap()
    }

    /// Create a test request with an `Authorization` header.
    fn test_req_with_auth_header(token: &str) -> http::Request<()> {
        let mut req = http::Request::builder().body(()).unwrap();

        let bearer_token = Authorization::bearer(token).expect("valid bearer token");
        req.headers_mut().typed_insert(bearer_token);

        req
    }

    /// Create a test request with an invalid `Authorization` header.
    ///
    /// The `Authorization` header is set to the given `token` value without the `Bearer ` prefix.
    fn test_req_with_invalid_auth_header(token: &str) -> http::Request<()> {
        let mut req = http::Request::builder().body(()).unwrap();

        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(token).expect("valid string"),
        );

        req
    }

    /// Create a test request with an `AuthToken` extension.
    fn test_req_with_auth_token_extension(auth: &str) -> http::Request<()> {
        let mut req = http::Request::builder().body(()).unwrap();

        let auth_token = AuthToken::StudioApiKey(Arc::new(APIKey {
            key: auth.into(),
            ..Default::default()
        }));
        req.extensions_mut().insert(auth_token);

        req
    }

    /// Deserialize a GraphQL response body.
    async fn deserialize_graphql_response_body<T>(
        body: &mut BoxBody,
    ) -> serde_json::Result<graphql_http::http::response::ResponseBody<T>>
    where
        for<'de> T: serde::Deserialize<'de>,
    {
        let body = hyper::body::to_bytes(body).await.expect("valid body");
        serde_json::from_slice(body.as_ref())
    }

    /// If the `Authorization` header is not present, the middleware should return an error response.
    #[tokio::test]
    async fn auth_header_is_not_present() {
        //* Given
        let auth_ctx = test_auth_ctx(None);

        let (mut svc, mut handle) =
            tower_test::mock::spawn_layer(RequireAuthorizationLayer::new(auth_ctx));

        let req = test_req_unauthenticated();

        //* When
        // The service must be ready before calling it
        handle.allow(1);
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service and await the response
        let res = svc.call(req).await;

        //* Then
        assert_matches!(res, Ok(mut res) => {
            assert_eq!(res.status(), http::StatusCode::OK);
            assert_eq!(
                res.headers().typed_get::<ContentType>(),
                Some(ContentType::json())
            );
            assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body.errors.len(), 1);
                assert_eq!(res_body.errors[0].message, "auth error: Missing Authorization header");
            });
        });
    }

    /// If the `Authorization` header is invalid, invalid header format, the middleware should return an error response.
    #[tokio::test]
    async fn auth_header_invalid_format() {
        //* Given
        let auth_ctx = test_auth_ctx(None);

        let (mut svc, mut handle) =
            tower_test::mock::spawn_layer(RequireAuthorizationLayer::new(auth_ctx));

        let req = test_req_with_invalid_auth_header("test-token");

        //* When
        // The service must be ready before calling it
        handle.allow(1);
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service and await the response
        let res = svc.call(req).await;

        //* Then
        assert_matches!(res, Ok(mut res) => {
            assert_eq!(res.status(), http::StatusCode::OK);
            assert_eq!(res.headers().typed_get::<ContentType>(), Some(ContentType::json()));
            assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body.errors.len(), 1);
                assert_eq!(res_body.errors[0].message, "auth error: Invalid Authorization header");
            });
        });
    }

    /// If the `Authorization` header is invalid, empty bearer token, the middleware should return an error response.
    #[tokio::test]
    async fn auth_header_bearer_token_is_empty() {
        //* Given
        let auth_ctx = test_auth_ctx(None);

        let (mut svc, mut handle) =
            tower_test::mock::spawn_layer(RequireAuthorizationLayer::new(auth_ctx));

        let req = test_req_with_auth_header("");

        //* When
        // The service must be ready before calling it
        handle.allow(1);
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service and await the response
        let res = svc.call(req).await;

        //* Then
        assert_matches!(res, Ok(mut res) => {
            assert_eq!(res.status(), http::StatusCode::OK);
            assert_eq!(res.headers().typed_get(), Some(ContentType::json()));
            assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body.errors.len(), 1);
                assert_eq!(res_body.errors[0].message, "auth error: Invalid bearer token: Not found");
            });
        });
    }

    /// If the `Authorization` header is invalid, invalid bearer token, the middleware should return an error response.
    #[tokio::test]
    async fn invalid_api_key_auth_token() {
        //* Given
        let invalid_api_key = "0123456789abcdef0123456789";

        let auth_ctx = test_auth_ctx(None);

        let (mut svc, mut handle) =
            tower_test::mock::spawn_layer(RequireAuthorizationLayer::new(auth_ctx));

        let req = test_req_with_auth_header(invalid_api_key);

        //* When
        // The service must be ready before calling it
        handle.allow(1);
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service and await the response
        let res = svc.call(req).await;

        //* Then
        assert_matches!(res, Ok(mut res) => {
            assert_eq!(res.status(), http::StatusCode::OK);
            assert_eq!(res.headers().typed_get::<ContentType>(), Some(ContentType::json()));
            assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body.errors.len(), 1);
                assert_eq!(res_body.errors[0].message, "auth error: Invalid bearer token: Invalid auth token");
            });
        });
    }

    /// If the `Authorization` header contains a valid bearer token, the middleware should insert an `AuthToken` extension into the request.
    #[tokio::test]
    async fn valid_api_key_auth_token() {
        //* Given
        let api_key = "0123456789abcdef0123456789abcdef";

        let auth_ctx = test_auth_ctx(Some(api_key));

        let (mut svc, mut handle) =
            tower_test::mock::spawn_layer(RequireAuthorizationLayer::new(auth_ctx));

        let req = test_req_with_auth_header(api_key);

        //* When
        // The service must be ready before calling it
        handle.allow(1);
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service
        svc.call(req);

        let (r, _) = handle
            .next_request()
            .await
            .expect("service received a request");

        //* Then
        assert_matches!(r.extensions().get::<AuthToken>(), Some(AuthToken::StudioApiKey(api_key)) => {
            assert_eq!(api_key.key, "0123456789abcdef0123456789abcdef");
        });
    }

    /// If the `AuthToken` extension is already present, the middleware should do nothing.
    #[tokio::test]
    async fn auth_token_extension_is_present() {
        //* Given
        let api_key = "test-api-key";
        let auth_ctx = test_auth_ctx(Some(api_key));

        let (mut svc, mut handle) =
            tower_test::mock::spawn_layer(RequireAuthorizationLayer::new(auth_ctx));

        let req = test_req_with_auth_token_extension(api_key);

        //* When
        // The service must be ready before calling it
        handle.allow(1);
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service
        svc.call(req);

        let (r, _) = handle
            .next_request()
            .await
            .expect("service received a request");

        //* Then
        assert_matches!(r.extensions().get::<AuthToken>(), Some(AuthToken::StudioApiKey(api_key)) => {
            assert_eq!(api_key.key, "test-api-key");
        });
    }

    /// The query settings extension should be inserted into the request when using the Studio API keys auth
    /// schema.
    #[tokio::test]
    async fn studio_api_key_query_settings_extension_is_inserted() {
        //* Given
        let api_key = "0123456789abcdef0123456789abcdef";

        let auth_ctx = test_auth_ctx(Some(api_key));

        let (mut svc, mut handle) =
            tower_test::mock::spawn_layer(RequireAuthorizationLayer::new(auth_ctx));

        let req = test_req_with_auth_header(api_key);

        //* When
        // The service must be ready before calling it
        handle.allow(1);
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service
        svc.call(req);

        let (r, _) = handle
            .next_request()
            .await
            .expect("service received a request");

        //* Then
        assert_matches!(r.extensions().get::<AuthToken>(), Some(AuthToken::StudioApiKey(api_key)) => {
            assert_eq!(api_key.key, "0123456789abcdef0123456789abcdef");
        });
        assert_matches!(r.extensions().get::<QuerySettings>(), Some(_));
    }
}
