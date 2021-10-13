mod bytes;
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
pub use std::{convert::TryInto, fmt, str::FromStr};
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
        defaults.with(fmt_layer.json()).init();
    } else {
        defaults.with(fmt_layer).init();
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

pub trait Reader {
    type Writer;
    fn new() -> (Self::Writer, Self);
}
