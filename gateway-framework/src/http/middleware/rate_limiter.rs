use std::{
    sync::{atomic, atomic::AtomicUsize, Arc},
    task::{Context, Poll},
    time::Duration,
};

use alloy_primitives::Address;
use axum::http::Request;
use dashmap::DashMap;
use tokio::time::MissedTickBehavior;
use tower::Service;

use self::future::ResponseFuture;
use crate::{errors::Error, graphql};

/// Rate limit settings.
///
/// The settings can be provided globally (e.g., via config) or via authorization
/// (e.g., subscription specific rate).
#[derive(Clone, Debug, Default)]
pub struct RateLimitSettings {
    /// The rate limit key.
    pub key: Address,
    /// The query rate in queries per minute.
    pub queries_per_minute: usize,
}

pub mod future {
    //! A future response for the [`RateLimiter`](super::RateLimiter) service.

    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    /// The future response kind.
    #[pin_project::pin_project(project = KindProj)]
    enum Kind<F> {
        InnerService(#[pin] F),
        Error {
            response: Option<axum::response::Response>,
        },
    }

    /// A future response for [`RateLimiter`](super::RateLimiter).
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
        pub fn from_service(future: F) -> Self {
            Self {
                kind: Kind::InnerService(future),
                _resp: std::marker::PhantomData,
            }
        }

        /// Create a new [`ResponseFuture`] from an error response.
        pub fn error(response: impl axum::response::IntoResponse) -> Self {
            Self {
                kind: Kind::Error {
                    response: Some(response.into_response()),
                },
                _resp: std::marker::PhantomData,
            }
        }
    }
}

/// A rate limiter service that limits the number of requests per minute.
///
/// The rate limiter uses the `RateLimitSettings` extension to determine the
/// rate limit for a request. If the rate limit is exceeded, the service
/// returns a GraphQL error response.
#[derive(Clone)]
pub struct RateLimiter<S> {
    inner: S,
    counters: Arc<DashMap<Address, AtomicUsize>>,
}

impl<S> RateLimiter<S> {
    pub fn new(inner: S, counters: Arc<DashMap<Address, AtomicUsize>>) -> Self {
        Self { inner, counters }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for RateLimiter<S>
where
    S: Service<Request<ReqBody>, Response = axum::response::Response>,
{
    type Response = axum::response::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future, axum::response::Response>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        if let Some(RateLimitSettings {
            key,
            queries_per_minute,
        }) = req.extensions().get::<RateLimitSettings>()
        {
            // Get the counter for the key, or initialize it if not tracked.
            // Note that counters are for 1 minute intervals
            // c6f70ddd-9dbe-462a-9214-da94b199a544
            let counter = self.counters.entry(*key).or_default();

            // Increment the counter and check if it's over the limit
            let count = counter.fetch_add(1, atomic::Ordering::Relaxed);
            if count >= *queries_per_minute {
                return ResponseFuture::error(graphql::error_response(Error::Auth(
                    anyhow::anyhow!("rate limit exceeded"),
                )));
            }
        }

        ResponseFuture::from_service(self.inner.call(req))
    }
}

/// A layer that adds the [`RateLimiter`] middleware.
///
/// The [`RateLimiter`] middleware limits the number of requests per minute. It uses the
/// `RateLimitSettings` extension to determine the rate limit for a request. If the rate
/// limit is exceeded, the middleware returns a GraphQL error response.
///
/// The rate limiter uses a `DashMap` to store the state of the counters. The counters,
/// indexed by the [`RateLimitSettings`] `key`, are reset every minute. a counter is 0, it
/// is removed from the map. After the counters, the map is shrunk to fit the number
/// of elements.
#[derive(Clone)]
pub struct AddRateLimiterLayer {
    /// The rate limiter counters.
    counters: Arc<DashMap<Address, AtomicUsize>>,
}

impl Default for AddRateLimiterLayer {
    /// Create a new `AddRateLimiterLayer` with a reset interval of 60 seconds.
    ///
    /// The rate limiter uses a `DashMap` to store the state of the counters. The counters are
    /// reset every minute. If a counter is 0, it is removed from the map. After the counters,
    /// the map is shrunk to fit the number of elements.
    ///
    /// This method creates a new `AddRateLimiterLayer` with an empty `DashMap`, and spawns a
    /// periodic task to reset the counters every 60 seconds.
    fn default() -> Self {
        Self::new_with_reset_interval(Duration::from_secs(60))
    }
}

impl AddRateLimiterLayer {
    /// Create a new `AddRateLimiterLayer`.
    ///
    /// The rate limiter uses a `DashMap` to store the state of the counters. The counters are
    /// reset every minute. If a counter is 0, it is removed from the map. After the counters,
    /// the map is shrunk to fit the number of elements.
    ///
    /// This method creates a new `AddRateLimiterLayer` with an empty `DashMap`, and spawns a
    /// periodic task to reset the counters every `interval`.
    pub fn new_with_reset_interval(interval: Duration) -> Self {
        let state = Arc::new(DashMap::new());

        // Reset counters every minute. If a counter is 0, remove it.
        // After the retain, shrink the map to fit the number of elements.
        // c6f70ddd-9dbe-462a-9214-da94b199a544
        let counters = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;

                counters.retain(|_, value: &mut AtomicUsize| {
                    // If the counter is 0, remove it
                    if value.load(atomic::Ordering::Relaxed) == 0 {
                        return false;
                    }

                    // Reset the counter to 0
                    value.store(0, atomic::Ordering::Relaxed);
                    true
                });

                // Shrink the map to fit the number of elements
                counters.shrink_to_fit();
            }
        });

        Self { counters: state }
    }

    /// Get the rate limiter counters.
    #[cfg(test)]
    pub fn counters(&self) -> Arc<DashMap<Address, AtomicUsize>> {
        self.counters.clone()
    }
}

impl<S> tower::layer::Layer<S> for AddRateLimiterLayer {
    type Service = RateLimiter<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimiter::new(inner, self.counters.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic, time::Duration};

    use alloy_primitives::Address;
    use assert_matches::assert_matches;
    use axum::{body::Body, http};
    use headers::{ContentType, HeaderMapExt};
    use http_body_util::BodyExt;
    use tokio_test::assert_ready_ok;

    use super::{AddRateLimiterLayer, RateLimitSettings};

    /// Helper function to parse an address string into an `Address`.
    fn test_address(addr: &str) -> Address {
        addr.parse().expect("valid address")
    }

    /// Create a test request with no `RateLimitSettings` extension.
    fn test_req_no_rate_limit_extension() -> http::Request<()> {
        http::Request::builder().body(()).unwrap()
    }

    /// Create a test request with `RateLimitSettings` extension.
    fn test_req(settings: RateLimitSettings) -> http::Request<()> {
        let mut req = http::Request::builder().body(()).unwrap();
        req.extensions_mut().insert(settings);
        req
    }

    /// Deserialize a GraphQL response body.
    async fn deserialize_graphql_response_body<T>(
        body: &mut Body,
    ) -> serde_json::Result<thegraph_graphql_http::http::response::ResponseBody<T>>
    where
        for<'de> T: serde::Deserialize<'de>,
    {
        let body = body.collect().await.expect("valid body").to_bytes();
        serde_json::from_slice(body.as_ref())
    }

    /// When the [`RateLimitSettings`] extension is not present, the request should not be
    /// rate limited.
    #[tokio::test]
    async fn no_rate_limited_request_is_handled() {
        //* Given

        // Create a new rate limiter layer with a reset interval of `Duration::MAX` so it does
        // not reset the counters during the test
        let layer = AddRateLimiterLayer::new_with_reset_interval(Duration::MAX);

        // Get the rate limit counters to check the counter value
        let rate_limit_counters = layer.counters();

        let (mut svc, mut handle) = tower_test::mock::spawn_layer(layer);
        handle.allow(10); // Allow the service to handle 10 requests

        let req = test_req_no_rate_limit_extension();

        //* When
        // The service must be ready before calling it
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service
        svc.call(req);

        //* Then
        // The service should have handled the request without rate limiting
        let r = handle.next_request().await;
        assert_matches!(r, Some((r, _)) => {
            assert_matches!(r.extensions().get::<RateLimitSettings>(), None);
        });

        // The rate limit counters should not have been updated
        assert_eq!(rate_limit_counters.len(), 0);
    }

    /// When the [`RateLimitSettings`] extension is present, the request should be rate limited.
    /// If the rate limit is not exceeded, the service should handle the request.
    #[tokio::test]
    async fn rate_limited_request_within_limit() {
        //* Given
        let settings = RateLimitSettings {
            key: test_address("0x7e85cd2be319b777be2dd77942b9471a7e0c9b25"),
            queries_per_minute: 5,
        };

        // Create a new rate limiter layer with a reset interval of `Duration::MAX` so it does
        // not reset the counters during the test
        let layer = AddRateLimiterLayer::new_with_reset_interval(Duration::MAX);

        // Get the rate limit counters to check the counter value
        let rate_limit_counters = layer.counters();

        let (mut svc, mut handle) = tower_test::mock::spawn_layer(layer);
        handle.allow(10); // Allow the service to handle 10 requests

        // Create 3 requests with the same `RateLimitSettings` extension that
        // should be handled by the service within the limit
        let requests = vec![
            test_req(settings.clone()),
            test_req(settings.clone()),
            test_req(settings.clone()),
        ];

        //* When
        // Handle the requests within the limit
        for req in requests {
            // The service must be ready before calling it
            assert_ready_ok!(svc.poll_ready());

            // Call the wrapped service
            svc.call(req);
        }

        //* Then
        // The service should have handled the requests
        for _ in 0..3 {
            let r = handle.next_request().await;
            assert_matches!(r, Some((r, _)) => {
                assert_matches!(r.extensions().get::<RateLimitSettings>(), Some(s) => {
                    assert_eq!(s.key, settings.key);
                    assert_eq!(s.queries_per_minute, settings.queries_per_minute);
                });
            });
        }

        // The rate limit counters should have been updated
        assert_eq!(rate_limit_counters.len(), 1);
        assert_matches!(rate_limit_counters.get(&settings.key), Some(counter) => {
            assert_eq!(counter.load(atomic::Ordering::Relaxed), 3);
        });
    }

    /// When the [`RateLimitSettings`] extension is present, the request should be rate limited.
    /// If the rate limit is exceeded, the service should return a GraphQL error response.
    #[tokio::test]
    async fn rate_limited_request_exceeds_limit() {
        //* Given
        let settings = RateLimitSettings {
            key: test_address("0x7e85cd2be319b777be2dd77942b9471a7e0c9b25"),
            queries_per_minute: 4,
        };

        // Create a new rate limiter layer with a reset interval of `Duration::MAX` so it does
        // not reset the counters during the test
        let layer = AddRateLimiterLayer::new_with_reset_interval(Duration::MAX);

        // Get the rate limit counters to check the counter value
        let rate_limit_counters = layer.counters();

        let (mut svc, mut handle) = tower_test::mock::spawn_layer(layer);
        handle.allow(10); // Allow the service to handle 10 requests

        let requests = vec![
            test_req(settings.clone()),
            test_req(settings.clone()),
            test_req(settings.clone()),
            test_req(settings.clone()),
        ];
        let invalid_req = test_req(settings.clone());

        //* When
        // Handle the requests within the limit
        for req in requests {
            // The service must be ready before calling it
            assert_ready_ok!(svc.poll_ready());

            // Call the wrapped service, ignore the response
            svc.call(req);
        }

        // The service should have handled the requests
        for _ in 0..4 {
            handle.next_request().await.expect("request handled");
        }

        // Handle the request that exceeds the limit
        // The service must be ready before calling it
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service
        let res = svc.call(invalid_req).await;

        //* Then
        assert_matches!(res, Ok(mut res) => {
            assert_eq!(res.status(), http::StatusCode::OK);
            assert_eq!(res.headers().typed_get(), Some(ContentType::json()));
            assert_matches!(deserialize_graphql_response_body::<()>(res.body_mut()).await, Ok(res_body) => {
                assert_eq!(res_body.errors.len(), 1);
                assert_eq!(res_body.errors[0].message, "auth error: rate limit exceeded");
            });
        });

        // The rate limit counters should have been updated
        assert_eq!(rate_limit_counters.len(), 1);
        assert_matches!(rate_limit_counters.get(&settings.key), Some(counter) => {
            assert_eq!(counter.load(atomic::Ordering::Relaxed), 5);
        });
    }

    /// When the [`RateLimitSettings`] extension is present, the request should be rate limited.
    /// One rate limit counter should be created for each key, and updated independently.
    #[tokio::test]
    async fn different_rate_limits_should_be_handled() {
        //* Given
        let settings_1_max = RateLimitSettings {
            key: test_address("0xbe2A4049c53d8919b0605354Ad7aE8ce6e22717c"),
            queries_per_minute: 1,
        };
        let settings_3_max = RateLimitSettings {
            key: test_address("0x7e85cd2be319b777be2dd77942b9471a7e0c9b25"),
            queries_per_minute: 3,
        };

        // Create a new rate limiter layer with a reset interval of `Duration::MAX` so it does
        // not reset the counters during the test
        let layer = AddRateLimiterLayer::new_with_reset_interval(Duration::MAX);

        // Get the rate limit counters to check the counter value
        let rate_limit_counters = layer.counters();

        let (mut svc, mut handle) = tower_test::mock::spawn_layer(layer);
        handle.allow(10); // Allow the service to handle 10 requests

        let requests = vec![
            test_req(settings_1_max.clone()),
            test_req(settings_3_max.clone()),
            test_req(settings_3_max.clone()),
            test_req(settings_1_max.clone()), // This will be rejected, but it will increment the counter
            test_req(settings_1_max.clone()), // This will be rejected, but it will increment the counter
            test_req(settings_3_max.clone()),
            test_req(settings_3_max.clone()), // This will be rejected, but it will increment the counter
        ];

        //* When
        // Handle the requests
        for req in requests {
            // The service must be ready before calling it
            assert_ready_ok!(svc.poll_ready());

            // Call the wrapped service, ignore the response
            svc.call(req);
        }

        //* Then
        // The service should have handled the requests within the limit
        let mut handled_requests = Vec::with_capacity(4);
        for _ in 0..4 {
            handled_requests.push(handle.next_request().await);
        }

        assert_matches!(&handled_requests[0], Some((r, _)) => {
            assert_matches!(r.extensions().get::<RateLimitSettings>(), Some(s) => {
                assert_eq!(s.key, settings_1_max.key);
                assert_eq!(s.queries_per_minute, settings_1_max.queries_per_minute);
            });
        });

        assert_matches!(&handled_requests[3], Some((r, _)) => {
            assert_matches!(r.extensions().get::<RateLimitSettings>(), Some(s) => {
                assert_eq!(s.key, settings_3_max.key);
                assert_eq!(s.queries_per_minute, settings_3_max.queries_per_minute);
            });
        });

        // Assert the rate limit counters were updated
        assert_eq!(rate_limit_counters.len(), 2);
        assert_matches!(rate_limit_counters.get(&settings_1_max.key), Some(counter) => {
            assert_eq!(counter.load(atomic::Ordering::Relaxed), 3);
        });
        assert_matches!(rate_limit_counters.get(&settings_3_max.key), Some(counter) => {
            assert_eq!(counter.load(atomic::Ordering::Relaxed), 4);
        });
    }

    /// The rate limit counters should be reset after the reset interval. If a request is made
    /// after the reset, the service should handle the request within the limit.
    #[tokio::test]
    async fn request_counters_should_be_reset() {
        //* Given
        let settings = RateLimitSettings {
            key: test_address("0x7e85cd2be319b777be2dd77942b9471a7e0c9b25"),
            queries_per_minute: 3,
        };

        // Create a new rate limiter layer with a reset interval of 1 second, so it resets the
        // counters every second
        let layer = AddRateLimiterLayer::new_with_reset_interval(Duration::from_millis(500));

        // Get the rate limit counters to check the counter value
        let rate_limit_counters = layer.counters();

        let (mut svc, mut handle) = tower_test::mock::spawn_layer(layer);
        handle.allow(10); // Allow the service to handle 10 requests

        // Create 4 requests with the same `RateLimitSettings` extension that will be
        // handled by the service within the first time interval
        let requests = vec![
            test_req(settings.clone()),
            test_req(settings.clone()),
            test_req(settings.clone()),
            test_req(settings.clone()), // This will be rejected, but it will increment the counter
        ];

        // Create a request with the same `RateLimitSettings` extension that will be handled
        // by the service after the first time interval
        let request_after_reset = test_req(settings.clone());

        //* When
        // Handle the requests within the limit
        for req in requests {
            // The service must be ready before calling it
            assert_ready_ok!(svc.poll_ready());

            // Call the wrapped service
            svc.call(req);
        }

        // The service should have handled the requests within the limit
        for _ in 0..3 {
            handle.next_request().await;
        }

        // Wait for the first time interval to reset the counters
        let before_reset_counters = rate_limit_counters
            .get(&settings.key)
            .map(|counter| counter.load(atomic::Ordering::Relaxed));

        tokio::time::sleep(Duration::from_millis(500)).await;

        let after_reset_counters = rate_limit_counters
            .get(&settings.key)
            .map(|counter| counter.load(atomic::Ordering::Relaxed));

        // Handle the request after the reset
        // The service must be ready before calling it
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service
        svc.call(request_after_reset);

        //* Then
        // Assert that the counters were reset
        assert_eq!(before_reset_counters, Some(4)); // 3 reqs handled + 1 req rejected
        assert_eq!(after_reset_counters, Some(0)); // Counter was reset, but not removed

        // Assert the request after the reset was handled
        assert_matches!(handle.next_request().await, Some(_));

        // The rate limit counters should be up-to-date
        assert_eq!(rate_limit_counters.len(), 1);
        assert_matches!(rate_limit_counters.get(&settings.key), Some(counter) => {
            assert_eq!(counter.load(atomic::Ordering::Relaxed), 1);
        });
    }

    /// When a counter associated with a key is 0, it should be removed from the counters map if
    /// the counter was not incremented during the last time interval.
    #[tokio::test]
    async fn request_counters_should_be_removed() {
        //* Given
        let settings = RateLimitSettings {
            key: test_address("0x7e85cd2be319b777be2dd77942b9471a7e0c9b25"),
            queries_per_minute: 3,
        };

        // Create a new rate limiter layer with a reset interval of 1 second, so it resets the
        // counters every 500 milliseconds
        let layer = AddRateLimiterLayer::new_with_reset_interval(Duration::from_millis(500));

        // Get the rate limit counters to check the counter value
        let rate_limit_counters = layer.counters();

        let (mut svc, mut handle) = tower_test::mock::spawn_layer(layer);
        handle.allow(10); // Allow the service to handle 10 requests

        // Create 4 requests with the same `RateLimitSettings` extension that will be
        // handled by the service within the first time interval
        let requests = vec![
            test_req(settings.clone()),
            test_req(settings.clone()),
            test_req(settings.clone()),
            test_req(settings.clone()), // This will be rejected, but it will increment the counter
        ];

        // Create a request with the same `RateLimitSettings` extension that will be handled
        // by the service after the first time interval
        let request_after_remove = test_req(settings.clone());

        //* When
        // Handle the requests within the limit
        for req in requests {
            // The service must be ready before calling it
            assert_ready_ok!(svc.poll_ready());

            // Call the wrapped service
            svc.call(req);
        }

        // The service should have handled the requests within the limit
        for _ in 0..3 {
            handle.next_request().await.expect("request handled");
        }

        // Wait for two time interval to reset the counters and remove the counter
        let before_reset_counters = rate_limit_counters
            .get(&settings.key)
            .map(|counter| counter.load(atomic::Ordering::Relaxed));

        tokio::time::sleep(Duration::from_millis(500)).await;

        let after_reset_counters = rate_limit_counters
            .get(&settings.key)
            .map(|counter| counter.load(atomic::Ordering::Relaxed));

        tokio::time::sleep(Duration::from_millis(500)).await;

        let after_remove_counters = rate_limit_counters
            .get(&settings.key)
            .map(|counter| counter.load(atomic::Ordering::Relaxed));

        // Handle the request after the remove
        // The service must be ready before calling it
        assert_ready_ok!(svc.poll_ready());

        // Call the wrapped service
        svc.call(request_after_remove);

        //* Then
        // Assert that the counters were updated
        assert_eq!(before_reset_counters, Some(4)); // 3 req handled + 1 req rejected
        assert_eq!(after_reset_counters, Some(0)); // Counter was reset
        assert_eq!(after_remove_counters, None); // Counter was removed

        // Assert the request after the remove was handled
        assert_matches!(handle.next_request().await, Some(_));

        // The rate limit counters should have been updated after the reset
        assert_eq!(rate_limit_counters.len(), 1);
        assert_matches!(rate_limit_counters.get(&settings.key), Some(counter) => {
            assert_eq!(counter.load(atomic::Ordering::Relaxed), 1);
        });
    }
}
