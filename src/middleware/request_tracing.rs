use std::{
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
    task::{Context, Poll},
};

use axum::http::{Request, Response};
use tower::Service;
use tracing::{
    field,
    instrument::{Instrument, Instrumented},
};

#[derive(Clone)]
pub struct RequestId(pub String);

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
    gateway_id: String,
    counter: Arc<AtomicU64>,
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

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        const CLOUDFLARE_RAY_ID: http::HeaderName = http::HeaderName::from_static("cf-ray");
        let ray_id = req
            .headers()
            .get(&CLOUDFLARE_RAY_ID)
            .and_then(|v| Some(v.to_str().ok()?.to_string()));
        let request_id = ray_id.unwrap_or_else(|| {
            let request_count = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
            format!("{}-{:x}", self.gateway_id, request_count)
        });

        req.extensions_mut().insert(RequestId(request_id.clone()));
        self.inner.call(req).instrument(tracing::info_span!(
            "client_request",
            request_id,
            selector = field::Empty,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct RequestTracingLayer {
    gateway_id: String,
    counter: Arc<AtomicU64>,
}

impl RequestTracingLayer {
    pub fn new(gateway_id: String) -> Self {
        Self {
            gateway_id,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<S> tower::layer::Layer<S> for RequestTracingLayer {
    type Service = RequestTracing<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestTracing {
            inner,
            gateway_id: self.gateway_id.clone(),
            counter: self.counter.clone(),
        }
    }
}
