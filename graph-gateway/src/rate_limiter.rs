use crate::graphql_error_response;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use eventuals::EventualExt;
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use lazy_static::lazy_static;
use prelude::*;
use prometheus;
use simple_rate_limiter::RateLimiter;
use std::{
    future::{self, Ready},
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};

pub struct RateLimiterMiddleware {
    pub rate_limiter: Arc<RateLimiter<String>>,
    pub key: fn(&ServiceRequest) -> String,
}

pub struct RateLimiterService<S> {
    service: Rc<S>,
    rate_limiter: Arc<RateLimiter<String>>,
    key: fn(&ServiceRequest) -> String,
}

impl<S> Transform<S, ServiceRequest> for RateLimiterMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse> + 'static,
{
    type Response = ServiceResponse;
    type Error = S::Error;
    type InitError = ();
    type Transform = RateLimiterService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        let rate_limiter = self.rate_limiter.clone();
        eventuals::timer(Duration::from_secs(1))
            .pipe(move |_| rate_limiter.rotate_slots())
            .forever();
        future::ready(Ok(RateLimiterService {
            service: Rc::new(service),
            rate_limiter: self.rate_limiter.clone(),
            key: self.key,
        }))
    }
}

impl<S> Service<ServiceRequest> for RateLimiterService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse> + 'static,
{
    type Response = ServiceResponse;
    type Error = S::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    #[tracing::instrument(skip(self, request))]
    fn call(&self, request: ServiceRequest) -> Self::Future {
        let service = Rc::clone(&self.service);
        let rate_limiter = self.rate_limiter.clone();
        let f = self.key;
        async move {
            let key = f(&request);
            let rate_limited = rate_limiter.check_limited(key.clone());
            tracing::trace!(%key, %rate_limited);
            if rate_limited {
                with_metric(&METRICS.rate_limited, &[&key], |c| c.inc());
                return Ok(ServiceResponse::new(
                    request.into_parts().0,
                    graphql_error_response("Too many requests, try again later"),
                ));
            }
            service.call(request).await
        }
        .boxed_local()
    }
}

#[derive(Clone)]
struct Metrics {
    rate_limited: prometheus::IntCounterVec,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
}

impl Metrics {
    fn new() -> Self {
        Self {
            rate_limited: prometheus::register_int_counter_vec!(
                "gateway_rate_limited_queries",
                "Rate limited queries",
                &["key"]
            )
            .unwrap(),
        }
    }
}
