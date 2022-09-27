use crate::chains::ethereum;
use anyhow;
use bip39;
use clap::Parser;
use hdwallet::{self, KeyChain as _};
use indexer_selection::SecretKey;
use prelude::*;
use rdkafka::config::ClientConfig as KafkaConfig;
use semver::Version;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

// TODO: Consider the security implications of passing mnemonics, passwords, etc. via environment variables or CLI arguments.

#[derive(Debug, Parser)]
#[clap()]
pub struct Opt {
    #[clap(
        long = "--mnemonic",
        env = "MNEMONIC",
        help = "Ethereum wallet mnemonic"
    )]
    pub signer_key: SignerKey,
    #[clap(long, env, help = "IPFS endpoint with access to the subgraph files")]
    pub ipfs: URL,
    #[clap(long, env, help = "Fisherman endpoint")]
    pub fisherman: Option<URL>,
    #[clap(
        long,
        env,
        help = "Ethereum provider URLs, format: '<network>=<block-time>,<rest-url>(,<ws-url>)?;...'\ne.g. rinkeby=15,https://eth-rinkeby.alchemyapi.io/v2/<api-key>"
    )]
    pub ethereum_providers: EthereumProviders,
    #[clap(long, env, help = "Network subgraph URL")]
    pub network_subgraph: URL,
    #[clap(long, env, help = "Network subgraph auth token")]
    pub network_subgraph_auth_token: String,
    #[clap(
        long,
        env,
        help = "API keys that won't be blocked for non-payment",
        default_value = "",
        use_delimiter = true
    )]
    pub special_api_keys: Vec<String>,
    #[clap(long, env, default_value = "")]
    pub restricted_deployments: RestrictedDeployments,
    #[clap(long, env, parse(try_from_str), help = "Format log output as JSON")]
    pub log_json: bool,
    #[clap(long, env, default_value = "5")]
    pub indexer_selection_retry_limit: usize,
    #[clap(long, env, default_value = "3.1")]
    pub query_budget_scale: f64,
    #[clap(long, env, default_value = "0.595")]
    pub query_budget_discount: f64,
    #[clap(
        long,
        env,
        help = "The number of processes per Gateway location. This is used when approximating worldwide query volume."
    )]
    pub replica_count: u64,
    #[clap(
        long,
        env,
        help = "The number of geographic Gateway locations. This is used when approximating worldwide query volume."
    )]
    pub location_count: u64,
    #[clap(long, env, default_value = "6700")]
    pub port: u16,
    #[clap(long, env, default_value = "7300")]
    pub metrics_port: u16,
    #[clap(
        long = "--ip-rate-limit-window",
        env = "IP_RATE_LIMIT_WINDOW",
        help = "Duration of IP rate limiting window in seconds",
        default_value = "10"
    )]
    pub ip_rate_limit_window_secs: u8,
    #[clap(long, env, help = "IP rate limit per window", default_value = "250")]
    pub ip_rate_limit: u16,
    #[clap(
        help = "Duration of API rate limiting window in seconds",
        long = "--api-rate-limit-window",
        env = "API_RATE_LIMIT_WINDOW",
        default_value = "10"
    )]
    pub api_rate_limit_window_secs: u8,
    #[clap(long, env, help = "API rate limit per window", default_value = "1000")]
    pub api_rate_limit: u16,
    #[clap(long, env, help = "Minimum indexer version", default_value = "0.0.0")]
    pub min_indexer_version: Version,
    #[clap(long, env, help = "GeoIP database path")]
    pub geoip_database: Option<PathBuf>,
    #[clap(
        long,
        env,
        help = "GeoIP blocked countries (ISO 3166-1 alpha-2 codes)",
        default_value = "",
        use_delimiter = true
    )]
    pub geoip_blocked_countries: Vec<String>,
    #[clap(long, env, help = "Subgraph studio admin url")]
    pub studio_url: URL,
    #[clap(long, env, help = "Subgraph studio auth")]
    pub studio_auth: String,
    #[clap(
        long,
        env,
        help = "Respect the payment state of API keys (disable for testnets)",
        parse(try_from_str)
    )]
    pub api_key_payment_required: bool,
    #[clap(long, env, help = "Redpanda broker domains")]
    pub redpanda_brokers: String,
    #[clap(long, env, help = "Security protocol")]
    pub redpanda_security_protocol: Option<String>,
    #[clap(long, env, help = "SASL mechanism", default_value = "SCRAM-SHA-256")]
    pub redpanda_sasl_mechanism: String,
    #[clap(long, env, help = "SASL user")]
    pub redpanda_sasl_user: Option<String>,
    #[clap(long, env, help = "SASL password")]
    pub redpanda_sasl_password: Option<String>,
    #[clap(long, env, help = "SSL ca location")]
    pub redpanda_ssl_ca: Option<String>,
    #[clap(long, env, help = "SSL cert location")]
    pub redpanda_ssl_cert: Option<String>,
    #[clap(long, env, help = "SSL key location")]
    pub redpanda_ssl_key: Option<String>,
}

impl Opt {
    pub fn kafka_config(&self) -> KafkaConfig {
        let mut config = KafkaConfig::new();
        config.set("bootstrap.servers", &self.redpanda_brokers);
        config.set("group.id", "graph-gateway");
        config.set("message.timeout.ms", "3000");
        config.set("queue.buffering.max.ms", "1000");
        config.set("queue.buffering.max.messages", "1000000");
        config.set("allow.auto.create.topics", "true");
        config.set("sasl.mechanism", &self.redpanda_sasl_mechanism);
        if let Some(security_protocol) = self.redpanda_security_protocol.as_ref() {
            config.set("security.protocol", security_protocol);
        }
        if let Some(sasl_username) = self.redpanda_sasl_user.as_ref() {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = self.redpanda_sasl_password.as_ref() {
            config.set("sasl.password", sasl_password);
        }
        if let Some(ssl_ca_location) = self.redpanda_ssl_ca.as_ref() {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_cert_location) = self.redpanda_ssl_cert.as_ref() {
            config.set("ssl.certificate.location", ssl_cert_location);
        }
        if let Some(ssl_key_location) = self.redpanda_ssl_key.as_ref() {
            config.set("ssl.key.location", ssl_key_location);
        }
        config
    }
}

pub struct SignerKey(pub SecretKey);

impl fmt::Debug for SignerKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SignerKey(..)")
    }
}

impl FromStr for SignerKey {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Wallet seed zeroized on drop
        let wallet_seed = bip39::Seed::new(
            &bip39::Mnemonic::from_phrase(s, bip39::Language::English)?,
            "",
        );
        let signer_key = hdwallet::DefaultKeyChain::new(
            hdwallet::ExtendedPrivKey::with_seed(wallet_seed.as_bytes()).expect("Invalid mnemonic"),
        )
        .derive_private_key(key_path("scalar/allocations").into())
        .expect("Failed to derive signer key")
        .0
        .private_key;
        Ok(SignerKey(
            // Convert between versions of secp256k1 lib.
            SecretKey::from_slice(signer_key.as_ref()).unwrap(),
        ))
    }
}

#[derive(Debug)]
pub struct EthereumProviders(pub Vec<ethereum::Provider>);

impl FromStr for EthereumProviders {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err_usage = "networks syntax: <network>=<block-time>,<rpc-url>;...";
        let providers = s.split(";").collect::<Vec<&str>>();
        if providers.is_empty() {
            return Err(err_usage);
        }
        providers
            .into_iter()
            .map(|provider| -> Option<ethereum::Provider> {
                let kv = provider.splitn(2, "=").collect::<Vec<&str>>();
                let (block_time, rpc) = kv.get(1)?.split_once(',')?;
                Some(ethereum::Provider {
                    network: kv.get(0)?.to_string(),
                    block_time: Duration::from_secs(block_time.parse::<u64>().ok()?),
                    rpc: rpc.parse::<URL>().ok()?,
                })
            })
            .collect::<Option<Vec<ethereum::Provider>>>()
            .map(EthereumProviders)
            .ok_or(err_usage)
    }
}

#[derive(Clone, Debug)]
pub struct RestrictedDeployments(pub HashMap<SubgraphDeploymentID, HashSet<Address>>);

impl FromStr for RestrictedDeployments {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err_usage = "restricted_deployments syntax: <deployment>=<indexer>+;...";
        let entries = s
            .split(";")
            .map(|deployment| {
                let (deployment, indexers) = deployment.split_once('=')?;
                let deployment = deployment.parse::<SubgraphDeploymentID>().ok()?;
                let indexers = indexers
                    .split(',')
                    .map(|i| i.parse::<Address>().ok())
                    .collect::<Option<HashSet<Address>>>()?;
                Some((deployment, indexers))
            })
            .collect::<Option<HashMap<SubgraphDeploymentID, HashSet<Address>>>>()
            .ok_or(err_usage)?;
        Ok(RestrictedDeployments(entries))
    }
}
