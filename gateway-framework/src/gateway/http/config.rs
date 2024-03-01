use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::{collections::BTreeMap, fmt::Display};

use alloy_primitives::{Address, U256};
use custom_debug::CustomDebug;
use secp256k1::SecretKey;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use url::Url;

use crate::auth::methods::api_keys::APIKey;
use crate::config::{fmt_optional_url, Hidden, HiddenSecretKey};

fn default_gateway_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct GatewayConfig {
    /// The Gateway unique identifier. This ID is used to identify the Gateway in
    /// the network and traceability purposes.
    ///
    /// If not provided a UUID is generated.
    #[serde(default = "default_gateway_id")]
    pub gateway_id: String,

    /// Executable name of the gateway implementation (e.g. "subgraph-gateway")
    pub executable_name: String,

    // Geo blocking
    pub geoip: GeoIpConfig,

    /// Chain aliases
    #[serde(default)]
    pub chain_aliases: BTreeMap<String, String>,

    /// Block cache chain configurations
    pub chains: Vec<chains::Config>,

    /// IPFS endpoint with access to the subgraph files
    #[debug(with = Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    pub ipfs: Url,

    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,

    /// Network subgraph query path
    #[debug(with = Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    pub network_subgraph: Url,

    /// L2 gateway to forward client queries to
    #[debug(with = fmt_optional_url)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub l2_gateway: Option<Url>,

    pub attestations: AttestationConfig,

    /// List of indexer addresses to block. This should only be used
    /// temprorarily, to compensate for indexer-selection imperfections.
    #[serde(default)]
    pub bad_indexers: Vec<Address>,

    /// Scalar TAP config (receipt signing)
    pub scalar: ScalarConfig,

    /// Target for indexer fees paid per request
    pub query_fees_target: f64,

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

    /// Subscriptions configuration
    pub subscriptions: Option<Subscriptions>,

    /// Respect the payment state of API keys (disable for testnets)
    pub api_key_payment_required: bool,

    /// Check payment state of client (disable for testnets)
    pub payment_required: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GeoIpConfig {
    /// GeoIP database path
    pub database: Option<PathBuf>,
    /// GeoIP blocked countries (ISO 3166-1 alpha-2 codes)
    #[serde(default)]
    pub blocked_countries: Vec<String>,
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

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
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
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct SubscriptionsDomain {
    pub chain_id: u64,
    pub contract: Address,
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
