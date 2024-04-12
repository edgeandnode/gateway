use std::task::{Context, Poll};

use axum::http::{Request, Response};
use tower::Service;
use tracing::{
    field,
    instrument::{Instrument, Instrumented},
};

/// Query tracing span target
const REQUEST_SPAN_TARGET: &str = "client_request";

/// Middleware that instruments client query request with a tracing span.
///
/// This middleware instruments the request future with a span:
///  - Target: `client_request`
///  - Name: `client request`
///  - Fields:
///    - `request_id`: The ID of the request
///    - `graph_env`: The graph environment of the request (e.g. `mainnet`, `testnet`, etc.)
///    - `selector`: The request selector (e.g. Subgraph DeploymentId or SubgraphId )
///
/// **Important**: This middleware should be used as the first layer in the request handling middleware stack.
#[derive(Debug, Clone)]
pub struct RequestTracing<S> {
    inner: S,
    env_id: String,
}

impl<S> RequestTracing<S> {
    /// Create a new [`RequestTracing`] middleware.
    pub fn new(inner: S, graph_env: String) -> Self {
        Self {
            inner,
            env_id: graph_env,
        }
    }
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
        // Create a tracing span for the client request and enter it. This way events created by the different layers
        // in the middleware stack are associated with it.
        let client_request_span = tracing::info_span!(
            target: REQUEST_SPAN_TARGET,
            "client request",  // name
            graph_env = %self.env_id,
            query_id = field::Empty,
            selector = field::Empty,
        )
        .entered();

        self.inner.call(req).instrument(client_request_span.clone())
    }
}

/// A layer that applies the [`RequestTracing`] middleware.
///
/// See [`RequestTracing`] for more details.
#[derive(Debug, Clone)]
pub struct RequestTracingLayer {
    graph_env: String,
}

impl RequestTracingLayer {
    pub fn new(graph_env: impl Into<String>) -> Self {
        Self {
            graph_env: graph_env.into(),
        }
    }
}

impl<S> tower::layer::Layer<S> for RequestTracingLayer {
    type Service = RequestTracing<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestTracing::new(inner, self.graph_env.clone())
    }
}
