use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc};
use std::task::{Context, Poll};

use axum::http::{HeaderName, HeaderValue, Request, Response};
use tower::Service;

/// Cloudflare Ray ID header name.
static CLOUDFLARE_RAY_ID: HeaderName = HeaderName::from_static("cf-ray");

/// An identifier for a query.
#[derive(Clone)]
pub struct QueryId(pub String);

impl QueryId {
    /// Create a new [`QueryId`] from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Create a new [`QueryId`] from a HeaderValue.
    ///
    /// If the header value is invalid, an empty string will be used.
    pub fn from_header_value(value: &HeaderValue) -> Self {
        Self(value.to_str().unwrap_or_default().to_string())
    }

    /// Create a new [`QueryId`] from the Gateway ID and a counter.
    pub fn new_from_gateway_id_and_count(gateway_id: &str, counter: u64) -> Self {
        Self(format!("{}-{:x}", gateway_id, counter))
    }
}

impl AsRef<str> for QueryId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Set query IDs on ingoing requests.
///
/// If the request has a `cf-ray` header, it will be used as the query ID. Otherwise, a new query ID
/// derived from the gateway ID and a counter will be used.
///
/// The middleware inserts the query ID into the request extensions.
#[derive(Clone, Debug)]
pub struct SetQueryId<S> {
    inner: S,
    gateway_id: String,
    counter: Arc<AtomicU64>,
}

impl<S> SetQueryId<S> {
    /// Create a new [`SetQueryId] middleware.
    pub fn new(inner: S, gateway_id: String, counter: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            gateway_id,
            counter,
        }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for SetQueryId<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        if req.extensions().get::<QueryId>().is_none() {
            let query_id = if let Some(ray_id) = req.headers().get(&CLOUDFLARE_RAY_ID) {
                QueryId::from_header_value(ray_id)
            } else {
                let query_count = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
                QueryId::new_from_gateway_id_and_count(&self.gateway_id, query_count)
            };

            // Set the query ID on the current span. The query tracing middleware sets the span's query_id
            // field to field::Empty.  We set it here to the actual query ID.
            tracing::span::Span::current().record("query_id", tracing::field::display(&query_id));

            // Set the query ID on the request extensions
            req.extensions_mut().insert(query_id);
        }

        self.inner.call(req)
    }
}

/// Set query id extensions.
///
/// This layer applies the [`SetQueryId`] middleware.
#[derive(Clone, Debug)]
pub struct SetQueryIdLayer {
    gateway_id: String,
    counter: Arc<AtomicU64>,
}

impl SetQueryIdLayer {
    /// Create a new [`SetQueryIdLayer`].
    pub fn new(gateway_id: impl Into<String>) -> Self {
        Self {
            gateway_id: gateway_id.into(),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<S> tower::layer::Layer<S> for SetQueryIdLayer {
    type Service = SetQueryId<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SetQueryId::new(inner, self.gateway_id.clone(), self.counter.clone())
    }
}

#[cfg(test)]
mod tests {
    use hyper::http;
    use tower::{Service, ServiceBuilder, ServiceExt};

    use super::{QueryId, SetQueryIdLayer};

    #[tokio::test]
    async fn cf_ray_header_is_present() {
        //* Given
        let gateway_id = "test-gateway";

        let (mock_svc, mut handle) =
            tower_test::mock::pair::<http::Request<&str>, http::Response<&str>>();
        let mut svc = ServiceBuilder::new()
            .layer(SetQueryIdLayer::new(gateway_id))
            .service(mock_svc);

        let req = http::Request::builder()
            .header("cf-ray", "test-cf-ray")
            .body("test")
            .unwrap();

        //* When
        // The service must be ready before calling it
        svc.ready().await.expect("service is ready");
        svc.call(req);

        let (r, _) = handle
            .next_request()
            .await
            .expect("service received a request");

        //* Then
        assert_eq!(r.headers().get("cf-ray").unwrap(), "test-cf-ray");
        assert_eq!(
            r.extensions().get::<QueryId>().unwrap().as_ref(),
            "test-cf-ray"
        );
    }

    #[tokio::test]
    async fn auto_incrementing_id() {
        //* Given
        let gateway_id = "fe3c0304-7383-48f4-9f3a-fc0cb37f55ba";

        let (mock_svc, mut handle) =
            tower_test::mock::pair::<http::Request<&str>, http::Response<&str>>();
        let mut svc = ServiceBuilder::new()
            .layer(SetQueryIdLayer::new(gateway_id))
            .service(mock_svc);

        let req1 = http::Request::builder().body("test").unwrap();
        let req2 = http::Request::builder().body("test").unwrap();

        //* When
        // The service must be ready before calling it
        svc.ready().await.expect("service is ready");
        svc.call(req1);

        // Wait for the service to be ready again before calling it
        svc.ready().await.expect("service is ready");
        svc.call(req2);

        let (r1, _) = handle
            .next_request()
            .await
            .expect("service received a request");
        let (r2, _) = handle
            .next_request()
            .await
            .expect("service received a request");

        //* Then
        assert_eq!(
            r1.extensions().get::<QueryId>().unwrap().as_ref(),
            "fe3c0304-7383-48f4-9f3a-fc0cb37f55ba-0"
        );
        assert_eq!(
            r2.extensions().get::<QueryId>().unwrap().as_ref(),
            "fe3c0304-7383-48f4-9f3a-fc0cb37f55ba-1"
        );
    }

    #[tokio::test]
    async fn query_id_extension_is_already_present() {
        //* Given
        let gateway_id = "unique-gateway-id";

        let (mock_svc, mut handle) =
            tower_test::mock::pair::<http::Request<&str>, http::Response<&str>>();
        let mut svc = ServiceBuilder::new()
            .layer(SetQueryIdLayer::new(gateway_id))
            .service(mock_svc);

        let expected_query_id = "fe3c0304-7383-48f4-9f3a-fc0cb37f55ba-0";
        let req = {
            let mut req = http::Request::builder().body("test").unwrap();
            req.extensions_mut().insert(QueryId::new(expected_query_id));
            req
        };

        //* When
        // The service must be ready before calling it
        svc.ready().await.expect("service is ready");
        svc.call(req);

        let (r, _) = handle
            .next_request()
            .await
            .expect("service received a request");

        //* Then
        assert_eq!(
            r.extensions().get::<QueryId>().unwrap().as_ref(),
            expected_query_id
        );
    }
}
