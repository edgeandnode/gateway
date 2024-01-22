use std::task::{Context, Poll};

use axum::http::{Request, Response};
use tower::Service;
use tracing::field;
use tracing::instrument::{Instrument, Instrumented};

use crate::reports;

/// Query tracing span target
const QUERY_SPAN_TARGET: &str = reports::CLIENT_QUERY_TARGET;

/// Middleware that instruments client query request with a tracing span.
///
/// This middleware instruments the request future with a span:
///  - Target: `client_query`
///  - Name: `client query`
///  - Fields:
///    - `query_id`: The query ID of the request
///    - `graph_env`: The graph environment of the request (e.g. `mainnet`, `testnet`, etc.)
///    - `selector`: The request selector (e.g. Subgraph DeploymentId or SubgraphId )
///
/// **Important**: This middleware should be used as the first layer in the query handling middleware stack.
#[derive(Debug, Clone)]
pub struct QueryTracing<S> {
    inner: S,
    env_id: String,
}

impl<S> QueryTracing<S> {
    /// Create a new [`QueryTracing`] middleware.
    pub fn new(inner: S, graph_env: String) -> Self {
        Self {
            inner,
            env_id: graph_env,
        }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for QueryTracing<S>
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
        // Create a tracin span for the client query and enter it. This way events created by the different layers
        // in the middleware stack are associated with it.
        let client_query_span = tracing::info_span!(
            target: QUERY_SPAN_TARGET,
            "client query",  // name
            graph_env = %self.env_id,
            query_id = field::Empty,
            selector = field::Empty,
        )
        .entered();

        self.inner.call(req).instrument(client_query_span.clone())
    }
}

/// A layer that applies the [`QueryTracing`] middleware.
///
/// See [`QueryTracing`] for more details.
#[derive(Debug, Clone)]
pub struct QueryTracingLayer {
    graph_env: String,
}

impl QueryTracingLayer {
    pub fn new(graph_env: impl Into<String>) -> Self {
        Self {
            graph_env: graph_env.into(),
        }
    }
}

impl<S> tower::layer::Layer<S> for QueryTracingLayer {
    type Service = QueryTracing<S>;

    fn layer(&self, inner: S) -> Self::Service {
        QueryTracing::new(inner, self.graph_env.clone())
    }
}
