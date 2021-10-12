use crate::{graphql_error_response, prelude::*};
use actix_web::{
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    http::StatusCode,
};
use eventuals::{self, EventualExt as _};
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    future::{self, Ready},
    iter::{self, FromIterator as _},
    mem,
    ops::DerefMut,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::sync::RwLock;

pub struct RateLimiterMiddleware {
    pub rate_limiter: Arc<RateLimiter>,
    pub key: fn(&ServiceRequest) -> String,
}

pub struct RateLimiterService<S> {
    service: Rc<S>,
    rate_limiter: Arc<RateLimiter>,
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
            let rate_limited = rate_limiter.check_limited(key.clone()).await;
            tracing::debug!(%key, %rate_limited);
            if rate_limited {
                return Ok(ServiceResponse::new(
                    request.into_parts().0,
                    graphql_error_response(StatusCode::OK, "Too many requests, try again later"),
                ));
            }
            service.call(request).await
        }
        .boxed_local()
    }
}

// If deemed necessary, the scalability of this rate limiter can be improved with either of the
// following:
// 1. Use a more optimistic concurrent map (based on something like a Ctrie)
// 2. Shard the `current_slot` map by chunks of the IP address space to facilitate parallel writes
//    to the current slot.

pub struct RateLimiter {
    limit: usize,
    slots: RwLock<VecDeque<HashMap<String, AtomicUsize>>>,
    current_slot: RwLock<HashMap<String, AtomicUsize>>,
}

impl RateLimiter {
    pub fn new(window: Duration, limit: usize) -> Arc<Self> {
        let slots: usize = 10;
        let slot_time = Duration::from_millis((window.as_millis() / (slots as u128)) as u64);
        let rate_limiter = Arc::new(Self {
            limit,
            slots: RwLock::new(VecDeque::from_iter(
                iter::repeat_with(|| HashMap::new()).take(slots),
            )),
            current_slot: RwLock::default(),
        });
        {
            let rate_limiter = rate_limiter.clone();
            eventuals::timer(slot_time)
                .pipe_async(move |_| {
                    let rate_limiter = rate_limiter.clone();
                    async move {
                        // Take the current slot data. Rotate the previous slots and place the
                        // current slot at the back.
                        // This automatically prunes IPs that are infrequently used.
                        let mut slots = rate_limiter.slots.write().await;
                        let front = slots
                            .pop_front()
                            .map(|mut front| {
                                front.clear();
                                front
                            })
                            .unwrap_or_default();
                        let current = mem::replace(
                            rate_limiter.current_slot.write().await.deref_mut(),
                            front,
                        );
                        slots.push_back(current);
                    }
                })
                .forever();
        }
        rate_limiter
    }

    async fn check_limited(&self, key: String) -> bool {
        // We want to avoid a situation where a maliciously overactive client can degrade the
        // ability for the gateway to serve other clients. So we limit the contention and mutually
        // exclusive locking that can be caused by such a client. A malicious client will most
        // often be limited based on their count from prior slots, which are infrequently modified.
        // If we need to check the current slot, then the malicious client will only be able to
        // trigger a write lock acquisition up to `limit` times in the worst case for the entire
        // window.
        let mut sum: usize = self
            .slots
            .read()
            .await
            .iter()
            .map(|slot| {
                slot.get(&key)
                    .map(|counter| counter.load(MemoryOrdering::Relaxed))
                    .unwrap_or(0)
            })
            .sum();
        // Don't increment if the limit is already reached from prior slots.
        if sum > self.limit {
            return true;
        }
        sum += self.increment(key).await;
        sum >= self.limit
    }

    async fn increment(&self, key: String) -> usize {
        if let Ok(map) = self.current_slot.try_read() {
            if let Some(counter) = map.get(&key) {
                return counter.fetch_add(1, MemoryOrdering::Relaxed);
            }
        }
        match self.current_slot.write().await.entry(key) {
            Entry::Occupied(entry) => entry.get().fetch_add(1, MemoryOrdering::Relaxed),
            Entry::Vacant(entry) => {
                entry.insert(AtomicUsize::new(1));
                1
            }
        }
    }
}
