//! The Graph Gateway configuration.

use std::{
    collections::BTreeMap,
    fmt::{self, Display},
    path::PathBuf,
};

use alloy_primitives::{Address, U256};
use custom_debug::CustomDebug;
use gateway_framework::{
    auth::methods::api_keys::APIKey,
    config::{Hidden, HiddenSecretKey},
};
use secp256k1::SecretKey;
use semver::Version;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use url::Url;

use crate::indexers::public_poi::ProofOfIndexingInfo;

#[serde_as]
#[derive(CustomDebug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub api_keys: Option<ApiKeys>,
    pub attestations: AttestationConfig,
    /// List of indexer addresses to block. This should only be used temprorarily, to compensate for
    /// indexer-selection imperfections.
    #[serde(default)]
    pub bad_indexers: Vec<Address>,
    /// Chain aliases
    #[serde(default)]
    pub chain_aliases: BTreeMap<String, String>,
    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,
    /// The Gateway unique identifier. This ID is used to identify the Gateway in the network
    /// and traceability purposes.
    ///
    /// If not provided a UUID is generated.
    #[serde(default)]
    pub gateway_id: Option<String>,
    /// Graph network environment identifier, inserted into Kafka messages
    pub graph_env_id: String,
    /// Rounds of indexer selection and queries to attempt. Note that indexer queries have a 20s
    /// timeout, so setting this to 5 for example would result in a 100s worst case response time
    /// for a client query.
    pub indexer_selection_retry_limit: usize,
    /// File path of CSV containing rows of `IpNetwork,Country`
    pub ip_blocker_db: Option<PathBuf>,
    /// IP rate limit in requests per second
    pub ip_rate_limit: u16,
    /// See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    #[serde(default)]
    pub kafka: KafkaConfig,
    /// Format log output as JSON
    pub log_json: bool,
    /// L2 gateway to forward client queries to
    #[debug(with = fmt_optional_url)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub l2_gateway: Option<Url>,
    /// Minimum graph-node version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_graph_node_version: Version,
    /// Minimum indexer-service version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_indexer_version: Version,
    /// Network subgraph query path
    #[debug(with = Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    pub network_subgraph: Url,
    /// Check payment state of client (disable for testnets)
    pub payment_required: bool,
    /// POI blocklist
    #[serde(default)]
    pub poi_blocklist: Vec<ProofOfIndexingInfo>,
    /// POI blocklist update interval in minutes (default: 20 minutes)
    pub poi_blocklist_update_interval: Option<u64>,
    /// public API port
    pub port_api: u16,
    /// private metrics port
    pub port_metrics: u16,
    /// Target for indexer fees paid per request
    pub query_fees_target: f64,
    /// Scalar TAP config (receipt signing)
    pub scalar: Scalar,
    /// Subscriptions configuration
    pub subscriptions: Option<Subscriptions>,
}

fn fmt_optional_url(url: &Option<Url>, f: &mut fmt::Formatter) -> fmt::Result {
    match url {
        Some(url) => write!(f, "Some({})", url),
        None => write!(f, "None"),
    }
}

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ApiKeys {
    Endpoint {
        /// URL where the API key info is served
        #[serde_as(as = "DisplayFromStr")]
        url: Url,
        /// Bearer auth token
        auth: Hidden<String>,
        /// API keys that won't be blocked for non-payment
        #[serde(default)]
        special: Vec<String>,
    },
    /// Fixed conversion rate of GRT/USD
    Fixed(Vec<APIKey>),
}

#[derive(Debug, Deserialize)]
pub struct AttestationConfig {
    pub chain_id: String,
    pub dispute_manager: Address,
}

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ExchangeRateProvider {
    /// Ethereum RPC provider
    Rpc(#[serde_as(as = "DisplayFromStr")] Url),
    /// Fixed conversion rate of GRT/USD
    Fixed(f64),
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig(BTreeMap<String, String>);

impl Default for KafkaConfig {
    fn default() -> Self {
        let settings = [
            ("bootstrap.servers", ""),
            ("group.id", "graph-gateway"),
            ("message.timeout.ms", "3000"),
            ("queue.buffering.max.ms", "1000"),
            ("queue.buffering.max.messages", "100000"),
        ];
        Self(
            settings
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        )
    }
}

impl From<KafkaConfig> for rdkafka::config::ClientConfig {
    fn from(mut from: KafkaConfig) -> Self {
        let mut settings = KafkaConfig::default().0;
        settings.append(&mut from.0);

        let mut config = rdkafka::config::ClientConfig::new();
        for (k, v) in settings {
            config.set(&k, &v);
        }
        config
    }
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct Scalar {
    /// Scalar TAP verifier contract chain
    pub chain_id: U256,
    /// Secret key for legacy voucher signing
    #[serde_as(as = "Option<HiddenSecretKey>")]
    pub legacy_signer: Option<Hidden<SecretKey>>,
    /// Secret key for voucher signing
    #[serde_as(as = "HiddenSecretKey")]
    pub signer: Hidden<SecretKey>,
    /// Scalar TAP verifier contract address
    pub verifier: Address,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct Subscriptions {
    /// Subscriptions contract domains
    pub domains: Vec<SubscriptionsDomain>,
    /// Query key signers that don't require payment
    #[serde(default)]
    pub special_signers: Vec<Address>,
    /// Subscriptions subgraph URL
    #[serde_as(as = "DisplayFromStr")]
    pub subgraph: Url,
    /// Subscriptions ticket for internal queries
    pub ticket: Option<String>,
    /// Subscription rate required per query per minute.
    /// e.g. If 0.01 USDC (6 decimals) is required per query per minute, then this should be set to
    /// 10000.
    pub rate_per_query: u128,
    /// Allow startup with no active subscriptions.
    #[serde(default)]
    pub allow_empty: bool,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct SubscriptionsDomain {
    pub chain_id: u64,
    pub contract: Address,
}
