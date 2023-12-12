use std::ops::Deref;
use std::str::FromStr;
use std::{collections::BTreeMap, fmt, path::PathBuf};

use alloy_primitives::{Address, B256, U256};
use graph_subscriptions::subscription_tier::{SubscriptionTier, SubscriptionTiers};
use prelude::UDecimal18;
use secp256k1::SecretKey;
use semver::Version;
use serde::Deserialize;
use serde_with::{serde_as, DeserializeAs, DisplayFromStr, FromInto};
use toolshed::url::Url;

use crate::indexers::public_poi::ProofOfIndexingInfo;

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Respect the payment state of API keys (disable for testnets)
    pub api_key_payment_required: bool,
    pub attestations: AttestationConfig,
    pub chains: Vec<Chain>,
    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,
    /// GeoIP database path
    pub geoip_database: Option<PathBuf>,
    /// GeoIP blocked countries (ISO 3166-1 alpha-2 codes)
    #[serde(default)]
    pub geoip_blocked_countries: Vec<String>,
    /// Graph network environment identifier, inserted into Kafka messages
    pub graph_env_id: String,
    /// Rounds of indexer selection and queries to attempt. Note that indexer queries have a 20s
    /// timeout, so setting this to 5 for example would result in a 100s worst case response time
    /// for a client query.
    pub indexer_selection_retry_limit: usize,
    /// IPFS endpoint with access to the subgraph files
    #[serde_as(as = "DisplayFromStr")]
    pub ipfs: Url,
    /// IP rate limit in requests per second
    pub ip_rate_limit: u16,
    /// See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    #[serde(default)]
    pub kafka: KafkaConfig,
    /// Format log output as JSON
    pub log_json: bool,
    /// L2 gateway to forward client queries to
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub l2_gateway: Option<Url>,
    /// Minimum indexer software version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_indexer_version: Version,
    /// Network subgraph query path
    #[serde_as(as = "DisplayFromStr")]
    pub network_subgraph: Url,
    /// POI blocklist
    #[serde(default)]
    pub poi_blocklist: Vec<ProofOfIndexingInfo>,
    /// POI blocklist update interval in minutes (default: 20 minutes)
    pub poi_blocklist_update_interval: Option<u64>,
    /// public API port
    pub port_api: u16,
    /// private metrics port
    pub port_metrics: u16,
    /// Target for indexer fees paid per query
    pub query_fees_target: f64,
    /// Scalar TAP config (receipt signing)
    pub scalar: Scalar,
    /// API keys that won't be blocked for non-payment
    #[serde(default)]
    pub special_api_keys: Vec<String>,
    /// Subgraph studio admin auth token
    pub studio_auth: String,
    /// Subgraph studio admin url
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub studio_url: Option<Url>,
    /// Subscriptions configuration
    pub subscriptions: Option<Subscriptions>,
}

#[derive(Debug, Deserialize)]
pub struct AttestationConfig {
    pub chain_id: String,
    pub dispute_manager: Address,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct Chain {
    /// The first name is used in logs, the others are aliases also supported in subgraph manifests.
    pub names: Vec<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub rpc: Url,
}

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ExchangeRateProvider {
    /// Ethereum RPC provider
    Rpc(#[serde_as(as = "DisplayFromStr")] Url),
    /// Fixed conversion rate of GRT/USD
    Fixed(#[serde_as(as = "DisplayFromStr")] UDecimal18),
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
    /// Kafka topic to report subscription queries
    pub kafka_topic: Option<String>,
    /// Query key signers that don't require payment
    pub special_signers: Vec<Address>,
    /// Subscriptions subgraph URL
    #[serde_as(as = "DisplayFromStr")]
    pub subgraph: Url,
    /// Subscriptions ticket for internal queries
    pub ticket: Option<String>,
    /// Subscription tiers
    #[serde(default)]
    #[serde_as(as = "FromInto<Vec<SubscriptionTier>>")]
    pub tiers: SubscriptionTiers,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct SubscriptionsDomain {
    pub chain_id: u64,
    pub contract: Address,
}

#[derive(Deserialize)]
#[serde(transparent)]
pub struct Hidden<T>(pub T);

impl<T: fmt::Debug> fmt::Debug for Hidden<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HIDDEN")
    }
}

impl<T: FromStr> FromStr for Hidden<T> {
    type Err = T::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl<T> Deref for Hidden<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct HiddenSecretKey;
impl<'de> DeserializeAs<'de, Hidden<SecretKey>> for HiddenSecretKey {
    fn deserialize_as<D>(deserializer: D) -> Result<Hidden<SecretKey>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = B256::deserialize(deserializer)?;
        SecretKey::from_slice(bytes.as_slice())
            .map(Hidden)
            .map_err(serde::de::Error::custom)
    }
}
