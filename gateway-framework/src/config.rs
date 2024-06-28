use std::{
    collections::BTreeMap,
    fmt,
    fmt::Display,
    ops::{Deref, DerefMut},
    path::PathBuf,
    str::FromStr,
};

use alloy_primitives::{Address, B256, U256};
use custom_debug::CustomDebug;
use secp256k1::SecretKey;
use serde::Deserialize;
use serde_with::{serde_as, DeserializeAs, DisplayFromStr};
use url::Url;

use crate::auth::api_keys::APIKey;

#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct Hidden<T>(pub T);

impl<T: std::fmt::Debug> std::fmt::Debug for Hidden<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

pub struct HiddenSecretKey;

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

pub fn fmt_optional_url(url: &Option<Url>, f: &mut fmt::Formatter) -> fmt::Result {
    match url {
        Some(url) => write!(f, "Some({})", url),
        None => write!(f, "None"),
    }
}

fn default_gateway_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct GatewayConfig {
    pub gateway_details: GatewayDetails,
    pub indexer_selection: IndexerSelectionConfig,
    pub payments: PaymentsConfig,
    pub chains: ChainsConfig,
    pub network: NetworkConfig,

    /// IPFS endpoint with access to the manifest files
    #[debug(with = Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    pub ipfs: Url,
    /// Main API port
    pub api_port: u16,
    /// Private metrics port
    pub metrics_port: u16,
    /// See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    #[serde(default)]
    pub kafka: KafkaConfig,
    /// Format log output as JSON
    pub log_json: bool,
    /// IP rate limit in requests per second
    pub ip_rate_limit: u16,
    /// API key configuration
    #[serde(default)]
    pub api_keys: Option<ApiKeys>,
    /// API keys that won't be blocked for non-payment
    #[serde(default)]
    pub special_api_keys: Vec<String>,
}

#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct GatewayDetails {
    /// The Gateway unique identifier. This ID is used to identify the Gateway
    /// in the network and traceability purposes. If not provided a UUID is
    /// generated.
    #[serde(default = "default_gateway_id")]
    pub id: String,
    /// Executable name of the gateway implementation (e.g. "subgraph-gateway")
    pub executable_name: String,
    /// L2 gateway to forward client queries to
    #[debug(with = fmt_optional_url)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub l2_gateway: Option<Url>,
}

#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct NetworkConfig {
    /// Graph network environment identifier, inserted into Kafka messages
    pub id: String,
    /// Network subgraph query path
    #[debug(with = Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    pub network_subgraph: Url,
    pub network_subgraph_auth_token: Option<String>,
    // Attestation configuration
    pub attestations: AttestationConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IndexerSelectionConfig {
    /// File path of CSV containing rows of `IpNetwork,Country`
    pub ip_blocker_db: Option<PathBuf>,
    /// List of indexer addresses to block. This should only be used
    /// temprorarily, to compensate for indexer-selection imperfections.
    #[serde(default)]
    pub bad_indexers: Vec<Address>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PaymentsConfig {
    /// Scalar TAP config (receipt signing)
    pub scalar: ScalarConfig,
    /// Target for indexer fees paid per request
    pub query_fees_target: f64,
    /// Check payment state of client (disable for testnets)
    pub payment_required: bool,
    /// Respect the payment state of API keys (disable for testnets)
    pub api_key_payment_required: bool,
    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ChainsConfig {
    /// Block cache chain configurations
    pub chains: Vec<chains::Config>,
    /// Chain aliases
    #[serde(default)]
    pub aliases: BTreeMap<String, String>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ExchangeRateProvider {
    /// Ethereum RPC provider
    Rpc(#[serde_as(as = "DisplayFromStr")] Url),
    /// Fixed conversion rate of GRT/USD
    Fixed(f64),
}

#[derive(Clone, Debug, Deserialize)]
pub struct AttestationConfig {
    pub chain_id: String,
    pub dispute_manager: Address,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct ScalarConfig {
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

#[derive(Clone, Debug, Deserialize)]
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

impl Deref for KafkaConfig {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for KafkaConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
#[derive(Clone, Debug, Deserialize)]
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
    /// Fixed API keys
    Fixed(Vec<APIKey>),
}
/// The block cache chain configuration.
pub mod chains {
    use std::fmt::Display;

    use custom_debug::CustomDebug;
    use serde::Deserialize;
    use serde_with::{serde_as, DisplayFromStr};
    use url::Url;

    /// The chain configuration.
    #[derive(Clone, Debug, Deserialize)]
    pub struct Config {
        /// Chain names.
        ///
        /// The first name is used in logs, the others are aliases also supported in subgraph
        /// manifests.
        pub names: Vec<String>,

        /// The RPC client type.
        #[serde(flatten)]
        pub rpc: RpcConfig,
    }

    /// The RPC configuration for a chain.
    #[serde_as]
    #[derive(Clone, CustomDebug, Deserialize)]
    #[serde(tag = "rpc_type")]
    #[serde(rename_all = "snake_case")]
    pub enum RpcConfig {
        Ethereum {
            /// The RPC URL for the chain.
            #[serde_as(as = "DisplayFromStr")]
            #[debug(with = "Display::fmt")]
            rpc_url: Url,
        },
        Blockmeta {
            /// The RPC URL for the chain.
            #[serde_as(as = "DisplayFromStr")]
            #[debug(with = "Display::fmt")]
            rpc_url: Url,

            /// The authentication token for the chain.
            #[debug(skip)]
            rpc_auth: String,
        },
    }

    #[cfg(test)]
    mod tests {
        use assert_matches::assert_matches;
        use serde_json::json;

        use super::{Config, RpcConfig};

        /// Test that deserializing a chain configuration with the previous format fails.
        /// The previous format was a single `rpc` field mapped to a URL, without the `rpc_type`
        /// field.
        #[test]
        fn previous_configuration_format_should_fail() {
            //* Given
            let expected_rpc_url = "http://localhost:8545/";

            let json_conf = json!({
                "names": ["ethereum", "eth"],
                "rpc": expected_rpc_url,
            });

            //* When
            let conf = serde_json::from_value::<Config>(json_conf);

            //* Then
            // Assert that the deserialization fails
            assert_matches!(conf, Err(err) => {
                assert!(err.to_string().contains("missing field `rpc_type`"));
            });
        }

        #[test]
        fn deserialize_valid_ethereum_rpc_config() {
            //* Given
            let expected_rpc_url = "http://localhost:8545/";

            let json_conf = json!({
                "names": ["ethereum", "eth"],
                "rpc_type": "ethereum",
                "rpc_url": expected_rpc_url
            });

            //* When
            let conf = serde_json::from_value::<Config>(json_conf);

            //* Then
            // Assert that the deserialized config is valid
            assert_matches!(conf, Ok(conf) => {
                assert_eq!(conf.names, vec!["ethereum", "eth"]);
                assert_matches!(conf.rpc, RpcConfig::Ethereum { rpc_url } => {
                    assert_eq!(rpc_url.as_str(), expected_rpc_url);
                });
            });
        }

        #[test]
        fn deserialize_valid_blockmeta_rpc_config() {
            //* Given
            let expected_rpc_url = "http://localhost:8545/";
            let expected_rpc_auth = "auth_token";

            let json_conf = json!({
                "names": ["blockmeta", "bm"],
                "rpc_type": "blockmeta",
                "rpc_url": expected_rpc_url,
                "rpc_auth": expected_rpc_auth
            });

            //* When
            let conf = serde_json::from_value::<Config>(json_conf);

            //* Then
            // Assert that the deserialized config is valid
            assert_matches!(conf, Ok(conf) => {
                assert_eq!(conf.names, vec!["blockmeta", "bm"]);
                assert_matches!(conf.rpc, RpcConfig::Blockmeta { rpc_url, rpc_auth } => {
                    assert_eq!(rpc_url.as_str(), expected_rpc_url);
                    assert_eq!(rpc_auth.as_str(), expected_rpc_auth);
                });
            });
        }

        #[test]
        fn deserialize_invalid_blockmeta_rpc_config_should_fail() {
            //* Given
            let expected_rpc_url = "http://localhost:8545/";

            let json_conf = json!({
                "names": ["blockmeta", "bm"],
                "rpc_type": "blockmeta",
                "rpc_url": expected_rpc_url
                // The `rpc_auth` field is missing
            });

            //* When
            let conf = serde_json::from_value::<Config>(json_conf);

            //* Then
            // Assert that the deserialization fails
            assert_matches!(conf, Err(err) => {
                assert!(err.to_string().contains("missing field `rpc_auth`"));
            });
        }

        #[test]
        fn deserialize_unknown_rpc_config_should_fail() {
            //* Given
            let expected_rpc_url = "http://localhost:8545/";

            let json_conf = json!({
                "names": ["blockmeta", "bm"],
                "rpc_type": "unknown",
                "rpc_url": expected_rpc_url
            });

            //* When
            let conf = serde_json::from_value::<Config>(json_conf);

            //* Then
            // Assert that the deserialization fails
            assert_matches!(conf, Err(err) => {
                assert!(err.to_string().contains("unknown variant"));
            });
        }

        #[test]
        fn blockmeta_rpc_config_auth_should_not_be_displayed() {
            //* Given
            let expected_rpc_url = "http://localhost:8545/";

            let rpc_config = RpcConfig::Blockmeta {
                rpc_url: expected_rpc_url.parse().expect("invalid url"),
                rpc_auth: "auth_token".to_string(),
            };

            //* When
            let debug_str = format!("{:?}", rpc_config);

            //* Then
            // Assert the `rpc_url` is properly displayed, and
            // the `rpc_auth` is not displayed
            assert!(debug_str.contains(expected_rpc_url));
            assert!(!debug_str.contains("auth_token"));
        }
    }
}
