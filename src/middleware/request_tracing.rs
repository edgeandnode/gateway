use std::task::{Context, Poll};

use axum::http::{Request, Response};
use tower::Service;
use tracing::{
    field,
    instrument::{Instrument, Instrumented},
};

/// Middleware that instruments client query request with a tracing span.
///
/// This middleware instruments the request future with a span:
///  - Target: `client_request`
///  - Name: `client request`
///  - Fields:
///    - `request_id`: The ID of the request
///    - `selector`: The request selector (e.g. Subgraph DeploymentId or SubgraphId )
///
/// **Important**: This middleware should be used as the first layer in the request handling middleware stack.
#[derive(Debug, Clone)]
pub struct RequestTracing<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RequestTracing<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Instrumented<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        self.inner.call(req).instrument(tracing::info_span!(
            "client_request",
            request_id = field::Empty,
            selector = field::Empty,
        ))
    }
}

/// A layer that applies the [`RequestTracing`] middleware.
///
/// See [`RequestTracing`] for more details.
#[derive(Debug, Clone)]
pub struct RequestTracingLayer;

impl<S> tower::layer::Layer<S> for RequestTracingLayer {
    type Service = RequestTracing<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestTracing { inner }
    }
}
