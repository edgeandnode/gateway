use std::{
    hash::{Hash, Hasher as _},
    time::SystemTime,
};

use siphasher::sip::SipHasher24;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

pub use crate::decimal::*;

pub mod buffer_queue;
pub mod decimal;
pub mod double_buffer;
pub mod epoch_cache;
pub mod test_utils;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

pub fn init_tracing(json: bool) {
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::try_new("info,graph_gateway=debug").unwrap()
    });
    let defaults = tracing_subscriber::registry().with(filter_layer);
    let fmt_layer = tracing_subscriber::fmt::layer();
    if json {
        defaults
            .with(fmt_layer.json().with_current_span(false))
            .init();
    } else {
        defaults.with(fmt_layer).init();
    }
}

/// Milliseconds since Unix epoch
pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn sip24_hash(value: &impl Hash) -> u64 {
    let mut hasher = SipHasher24::default();
    value.hash(&mut hasher);
    hasher.finish()
}

// The following are cumbersome by design. It's better to be forced to think hard about converting
// between these types.

/// USD with 18 fractional digits
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct USD(pub UDecimal18);
/// GRT with 18 fractional digits
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct GRT(pub UDecimal18);
