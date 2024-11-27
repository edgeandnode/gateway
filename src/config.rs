//! The Graph Gateway configuration.

use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::Context;
use ipnetwork::IpNetwork;
use ordered_float::NotNan;
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thegraph_core::{
    alloy::primitives::{Address, BlockNumber, B256, U256},
    DeploymentId,
};
use url::Url;

use crate::{auth::APIKey, network::subgraph_client::TrustedIndexer};

/// The Graph Gateway configuration.
#[serde_as]
#[derive(Deserialize)]
pub struct Config {
    #[serde(default)]
    pub api_keys: Option<ApiKeys>,
    pub attestations: AttestationConfig,
    /// List of indexer addresses to block. This should only be used temprorarily.
    #[serde(default)]
    pub blocked_indexers: BTreeMap<Address, BlockedIndexer>,
    /// Chain aliases
    #[serde(default)]
    pub chain_aliases: BTreeMap<String, String>,
    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,
    /// Graph network environment identifier, inserted into Kafka messages
    pub graph_env_id: String,
    /// File path of CSV containing rows of `IpNetwork,Country`
    pub ip_blocker_db: Option<PathBuf>,
    /// See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
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
    /// Indexers used to query the network subgraph
    pub trusted_indexers: Vec<TrustedIndexer>,
    /// Check payment state of client (disable for testnets)
    pub payment_required: bool,
    /// POI blocklist
    #[serde(default)]
    pub poi_blocklist: Vec<BlockedPoi>,
    /// public API port
    pub port_api: u16,
    /// private metrics port
    pub port_metrics: u16,
    /// Target for indexer fees paid per request
    #[serde(deserialize_with = "deserialize_not_nan_f64")]
    pub query_fees_target: NotNan<f64>,
    pub receipts: Receipts,
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
    /// Fixed set of API keys
    Fixed(Vec<APIKey>),
}

#[derive(Deserialize)]
pub struct BlockedIndexer {
    /// empty array blocks on all deployments
    pub deployments: Vec<DeploymentId>,
    pub reason: String,
}

/// Attestation configuration.
///
/// See [`Config`]'s [`attestations`](struct.Config.html#structfield.attestations).
#[derive(Deserialize)]
pub struct AttestationConfig {
    pub chain_id: String,
    pub dispute_manager: Address,
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
#[derive(Deserialize)]
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
    /// Secret key for legacy voucher signing (Scalar)
    pub legacy_signer: Option<B256>,
    /// TAP signer key
    pub signer: B256,
    /// TAP verifier contract address
    pub verifier: Address,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockedPoi {
    pub public_poi: B256,
    pub deployment: DeploymentId,
    pub block_number: BlockNumber,
    /// Example query (should be minimal to reproduce bad response)
    pub query: Option<String>,
    /// Bad query response, from the above query executed on indexers with this blocked PoI
    pub bad_query_response: Option<String>,
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
