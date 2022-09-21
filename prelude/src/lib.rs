pub mod buffer_queue;
pub mod bytes;
pub mod clock;
pub mod decimal;
pub mod double_buffer;
pub mod epoch_cache;
pub mod graphql;
pub mod test_utils;
pub mod weighted_sample;

pub use crate::{bytes::*, decimal::*};
pub use anyhow;
pub use eventuals::{self, Eventual, EventualWriter, Ptr};
pub use reqwest;
use serde::Deserialize;
use siphasher::sip::SipHasher24;
pub use std::{cmp::Ordering, fmt, str::FromStr};
use std::{
    hash::{Hash, Hasher as _},
    ops::Deref,
};
pub use tokio::{
    self,
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

impl<'de, const P: u8> serde::Deserialize<'de> for UDecimal<P> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input: &str = serde::Deserialize::deserialize(deserializer)?;
        input.parse::<Self>().map_err(serde::de::Error::custom)
    }
}

impl<const P: u8> serde::Serialize for UDecimal<P> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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

#[derive(Clone)]
pub struct URL(pub reqwest::Url);

impl fmt::Debug for URL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for URL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for URL {
    type Err = <reqwest::Url as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        reqwest::Url::from_str(s).map(Self)
    }
}

impl Deref for URL {
    type Target = reqwest::Url;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<reqwest::Url> for URL {
    fn from(from: reqwest::Url) -> Self {
        URL(from)
    }
}
