//! Gateway Configuration
//!
//! JSON-based configuration loaded at startup from a file path passed as a CLI argument.
//!
//! # Example Configuration
//!
//! ```json
//! {
//!   "api_keys": {
//!     "url": "https://api.example.com/gateway-api-keys",
//!     "auth": "Bearer <token>",
//!     "special": ["admin-key-1"]
//!   },
//!   "attestations": {
//!     "chain_id": "42161",
//!     "dispute_manager": "0x...",
//!     "legacy_dispute_manager": "0x..."
//!   },
//!   "blocklist": [
//!     { "deployment": "Qm...", "public_poi": "0x...", "block": 12345678 },
//!     { "deployment": "Qm...", "indexer": "0x..." }
//!   ],
//!   "chain_aliases": { "mainnet": "ethereum" },
//!   "exchange_rate_provider": "https://eth-mainnet.g.alchemy.com/v2/<key>",
//!   "graph_env_id": "production",
//!   "ip_blocker_db": "/path/to/ip-blocklist.csv",
//!   "kafka": {
//!     "bootstrap.servers": "localhost:9092",
//!     "security.protocol": "SASL_SSL"
//!   },
//!   "log_json": true,
//!   "min_graph_node_version": "0.35.0",
//!   "min_indexer_version": "1.0.0",
//!   "trusted_indexers": [
//!     { "url": "https://indexer.example.com/", "auth": "Bearer <token>" }
//!   ],
//!   "payment_required": true,
//!   "port_api": 8000,
//!   "port_metrics": 8001,
//!   "query_fees_target": 0.0001,
//!   "receipts": {
//!     "chain_id": "42161",
//!     "payer": "0x...",
//!     "signer": "0x<private_key_hex>",
//!     "verifier": "0x...",
//!     "legacy_verifier": "0x..."
//!   },
//!   "subgraph_service": "0x..."
//! }
//! ```
//!
//! # API Keys Configuration
//!
//! The `api_keys` field supports three variants:
//!
//! 1. **Endpoint**: Fetch from HTTP endpoint (polls periodically)
//! 2. **KafkaTopic**: Stream from Kafka topic (note: typo `KakfaTopic` preserved for compatibility)
//! 3. **Fixed**: Static list of API keys for testing
//!
//! # IP Blocklist
//!
//! The optional `ip_blocker_db` field points to a CSV file with format:
//! ```csv
//! 192.168.1.0/24,US
//! 10.0.0.0/8,Internal
//! ```
//!
//! Only the IP network (first column) is used; the country/label is ignored.

use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::Context;
use ipnetwork::IpNetwork;
use ordered_float::NotNan;
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, B256, BlockNumber, U256},
};
use url::Url;

use crate::{auth::ApiKey, network::subgraph_client::TrustedIndexer};

/// The Graph Gateway configuration.
#[serde_as]
#[derive(Deserialize)]
pub struct Config {
    pub api_keys: ApiKeys,
    pub attestations: AttestationConfig,
    /// Blocklist applying to indexers.
    #[serde(default)]
    pub blocklist: Vec<BlocklistEntry>,
    /// Chain aliases
    #[serde(default)]
    pub chain_aliases: BTreeMap<String, String>,
    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,
    /// Graph network environment identifier, inserted into Kafka messages
    pub graph_env_id: String,
    /// File path of CSV containing rows of `IpNetwork,Country`
    pub ip_blocker_db: Option<PathBuf>,
    /// See <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>
    #[serde(default)]
    pub kafka: KafkaConfig,
    /// Format log output as JSON
    pub log_json: bool,
    /// Minimum graph-node version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_graph_node_version: Version,
    /// Minimum indexer-service version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_indexer_version: Version,
    /// Trusted indexers that can serve the network subgraph for free
    pub trusted_indexers: Vec<TrustedIndexer>,
    /// Maximum acceptable lag (in seconds) for network subgraph responses (default: 120)
    #[serde(default = "default_network_subgraph_max_lag_seconds")]
    pub network_subgraph_max_lag_seconds: u64,
    /// Check payment state of client (disable for testnets)
    pub payment_required: bool,
    /// public API port
    pub port_api: u16,
    /// private metrics port
    pub port_metrics: u16,
    /// Target for indexer fees paid per request
    #[serde(deserialize_with = "deserialize_not_nan_f64")]
    pub query_fees_target: NotNan<f64>,
    pub receipts: Receipts,
    /// Address for the Subgraph Service
    pub subgraph_service: Address,
}

/// Default network subgraph max lag threshold (120 seconds)
fn default_network_subgraph_max_lag_seconds() -> u64 {
    120
}

/// Deserialize a `NotNan<f64>` from a `f64` and return an error if the value is NaN.
fn deserialize_not_nan_f64<'de, D>(deserializer: D) -> Result<NotNan<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = f64::deserialize(deserializer)?;
    NotNan::new(value).map_err(serde::de::Error::custom)
}

/// API keys configuration.
///
/// See [`Config`]'s [`api_keys`](struct.Config.html#structfield.api_keys).
#[serde_as]
#[derive(Deserialize)]
#[serde(untagged)]
pub enum ApiKeys {
    Endpoint {
        /// URL where the API key info is served
        #[serde_as(as = "DisplayFromStr")]
        url: Url,
        /// Bearer auth token
        auth: String,
        /// API keys that won't be blocked for non-payment
        #[serde(default)]
        special: Vec<String>,
    },
    KakfaTopic {
        topic: String,
        #[serde_as(as = "DisplayFromStr")]
        bootstrap_url: Url,
        bootstrap_auth: String,
        /// API keys that won't be blocked for non-payment
        #[serde(default)]
        special: Vec<String>,
    },
    /// Fixed set of API keys
    Fixed(Vec<ApiKey>),
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BlocklistEntry {
    Poi {
        deployment: DeploymentId,
        public_poi: B256,
        block: BlockNumber,
    },
    Other {
        deployment: DeploymentId,
        indexer: Address,
    },
}

/// Attestation configuration.
///
/// See [`Config`]'s [`attestations`](struct.Config.html#structfield.attestations).
#[derive(Deserialize)]
pub struct AttestationConfig {
    pub chain_id: String,
    pub dispute_manager: Address,
    pub legacy_dispute_manager: Address,
}

/// The exchange rate provider.
///
/// See [`Config`]'s [`exchange_rate_provider`](struct.Config.html#structfield.exchange_rate_provider).
#[serde_as]
#[derive(Deserialize)]
#[serde(untagged)]
pub enum ExchangeRateProvider {
    /// Ethereum RPC provider
    Rpc(#[serde_as(as = "DisplayFromStr")] Url),
    /// Fixed conversion rate of GRT/USD
    Fixed(#[serde(deserialize_with = "deserialize_not_nan_f64")] NotNan<f64>),
}

/// Kafka configuration.
///
/// See [`Config`]'s [`kafka`](struct.Config.html#structfield.kafka).
#[derive(Clone, Deserialize)]
pub struct KafkaConfig(BTreeMap<String, String>);

impl Default for KafkaConfig {
    fn default() -> Self {
        let settings = [
            ("bootstrap.servers", ""),
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

#[derive(Deserialize)]
pub struct Receipts {
    /// TAP verifier contract chain
    pub chain_id: U256,
    /// TAP payer address
    pub payer: Address,
    /// TAP signer key
    pub signer: B256,
    /// TAP verifier contract address (v2)
    pub verifier: Address,
    /// Legacy TAP verifier contract address (v1)
    pub legacy_verifier: Address,
}

/// Load the configuration from a JSON file.
pub fn load_from_file(path: &Path) -> anyhow::Result<Config> {
    let config_content = std::fs::read_to_string(path)?;
    let config = serde_json::from_str(&config_content)?;
    Ok(config)
}

/// Load the IP blocklist from a CSV file.
///
/// The CSV file should contain rows of `IpNetwork,Country`.
pub fn load_ip_blocklist_from_file(path: &Path) -> anyhow::Result<HashSet<IpNetwork>> {
    let db = std::fs::read_to_string(path).context("IP blocklist DB")?;
    Ok(db
        .lines()
        .filter_map(|line| line.split_once(',')?.0.parse().ok())
        .collect())
}
