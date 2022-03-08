pub mod bytes;
pub mod decimal;
pub mod shared_lookup;
pub mod weighted_sample;

#[cfg(test)]
pub mod test_utils;

pub use crate::prelude::{bytes::*, decimal::*};
pub use eventuals::{Eventual, EventualWriter, Ptr};
pub use prometheus::{
    self,
    core::{MetricVec, MetricVecBuilder},
};
use siphasher::sip::SipHasher24;
use std::hash::{Hash, Hasher as _};
pub use std::{cmp::Ordering, fmt, str::FromStr};
pub use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, Instant},
};
pub use tracing::{self, Instrument};
use tracing_subscriber::{self, layer::SubscriberExt as _, util::SubscriberInitExt as _};

pub fn init_tracing(json: bool) {
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or(tracing_subscriber::EnvFilter::try_new("info,graph_gateway=debug").unwrap());
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

pub fn sip24_hash(value: &impl Hash) -> u64 {
    let mut hasher = SipHasher24::default();
    value.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone)]
pub struct ResponseMetrics {
    pub duration: prometheus::Histogram,
    pub ok: prometheus::IntCounter,
    pub failed: prometheus::IntCounter,
}

impl ResponseMetrics {
    pub fn new(prefix: &str, description: &str) -> Self {
        Self {
            duration: prometheus::register_histogram!(
                &format!("{}_duration", prefix),
                &format!("Duration for {}", description),
            )
            .unwrap(),
            ok: prometheus::register_int_counter!(
                &format!("{}_ok", prefix),
                &format!("Successful {}", description),
            )
            .unwrap(),
            failed: prometheus::register_int_counter!(
                &format!("{}_failed", prefix),
                &format!("Failed {}", description),
            )
            .unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct ResponseMetricVecs {
    pub duration: prometheus::HistogramVec,
    pub ok: prometheus::IntCounterVec,
    pub failed: prometheus::IntCounterVec,
}

impl ResponseMetricVecs {
    pub fn new(prefix: &str, description: &str, labels: &[&str]) -> Self {
        Self {
            duration: prometheus::register_histogram_vec!(
                &format!("{}_duration", prefix),
                &format!("Duration for {}", description),
                labels,
            )
            .unwrap(),
            ok: prometheus::register_int_counter_vec!(
                &format!("{}_ok", prefix),
                &format!("Successful {}", description),
                labels,
            )
            .unwrap(),
            failed: prometheus::register_int_counter_vec!(
                &format!("{}_failed", prefix),
                &format!("Failed {}", description),
                labels,
            )
            .unwrap(),
        }
    }
}

pub fn with_metric<T, F, B>(metric_vec: &MetricVec<B>, label_values: &[&str], f: F) -> Option<T>
where
    B: MetricVecBuilder,
    F: Fn(B::M) -> T,
{
    metric_vec
        .get_metric_with_label_values(label_values)
        .ok()
        .map(f)
}

/// Encode the given name into a valid BIP-32 key chain path.
pub fn key_path(name: &str) -> String {
    std::iter::once("m".to_string())
        .chain(name.bytes().map(|b| b.to_string()))
        .collect::<Vec<String>>()
        .join("/")
}

/// Decimal Parts-Per-Million with 6 fractional digits
pub type PPM = UDecimal<6>;
/// Decimal USD with 18 fractional digits
pub type USD = UDecimal<18>;
/// Decimal GRT with 18 fractional digits
pub type GRT = UDecimal<18>;
/// Decimal GRT Wei (10^-18 GRT)
pub type GRTWei = UDecimal<0>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockPointer {
    pub number: u64,
    pub hash: Bytes32,
}

impl PartialOrd for BlockPointer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlockPointer {
    fn cmp(&self, other: &Self) -> Ordering {
        self.number.cmp(&other.number)
    }
}

#[derive(Debug, Clone)]
pub struct BlockHead {
    pub block: BlockPointer,
    pub uncles: Vec<Bytes32>,
}

pub trait Reader {
    type Writer;
    fn new() -> (Self::Writer, Self);
}
