use crate::graphql_error_response;
use actix_web::{
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    http::StatusCode,
};
use eventuals::{self, EventualExt as _};
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use lazy_static::lazy_static;
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    future::{self, Ready},
    iter::{self, FromIterator as _},
    mem,
    net::IpAddr,
    ops::DerefMut,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering as MemoryOrdering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{sync::RwLock, time::Duration};

pub struct RateLimiterMiddleware;

pub struct RateLimiterService<S> {
    service: Rc<S>,
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
        lazy_static! {
            static ref RATE_LIMITER: Arc<RateLimiter> =
                RateLimiter::new(Duration::from_secs(10), 250);
        }
        let service = Rc::clone(&self.service);
        async move {
            let rate_limited = match request.peer_addr() {
                Some(addr) => RATE_LIMITER.check_limited(addr.ip()).await,
                None => false,
            };
            tracing::trace!(addr = ?request.peer_addr(), %rate_limited);
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

// Replacing IpAddr with a type constrained by the following caused horrible lifetime issues. This
// problem should be resolved when this is used for rate limiting API keys as well.
// pub trait RateLimiterKey: Hash + Eq + Copy + Send + Sync {}

struct RateLimiter {
    limit: usize,
    slots: RwLock<VecDeque<HashMap<IpAddr, AtomicUsize>>>,
    current_slot: RwLock<HashMap<IpAddr, AtomicUsize>>,
}

impl RateLimiter {
    fn new(window: Duration, limit: usize) -> Arc<Self> {
        let slot_time = Duration::from_millis(50);
        let slots = (window.as_millis() as usize) / (slot_time.as_millis() as usize);
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
                .map(move |_| {
                    let rate_limiter = rate_limiter.clone();
                    async move {
                        // Take the current slot data. Rotate the previous slots and place the
                        // current slot at the back.
                        // This automatically prunes IPs that are infrequently used.
                        let current =
                            mem::take(rate_limiter.current_slot.write().await.deref_mut());
                        let mut slots = rate_limiter.slots.write().await;
                        slots.pop_front();
                        slots.push_back(current);
                    }
                })
                .pipe(|_| ())
                .forever();
        }
        rate_limiter
    }

    async fn check_limited(&self, key: IpAddr) -> bool {
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

    async fn increment(&self, key: IpAddr) -> usize {
        if let Some(counter) = self.current_slot.read().await.get(&key) {
            return counter.fetch_add(1, MemoryOrdering::Relaxed);
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
