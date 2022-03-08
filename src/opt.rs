use crate::{ethereum_client, indexer_selection::SecretKey, prelude::*};
use bip39;
use hdwallet::{self, KeyChain as _};
use ordered_float::NotNan;
use rdkafka::config::ClientConfig as KafkaConfig;
use std::{collections::HashMap, error::Error};
use structopt_derive::StructOpt;
use url::{self, Url};

// TODO: Consider the security implications of passing mnemonics, passwords, etc. via environment variables or CLI arguments.

#[derive(StructOpt, Debug)]
pub struct Opt {
    #[structopt(
        help = "Ethereum wallet mnemonic",
        long = "--mnemonic",
        env = "MNEMONIC"
    )]
    pub signer_key: SignerKey,
    #[structopt(
        help = "URL of gateway agent syncing API",
        long = "--sync-agent",
        env = "SYNC_AGENT"
    )]
    pub sync_agent: String,
    #[structopt(
        help = "Accept empty values from the gateway agent syncing API (useful for testing)",
        long = "--sync-agent-accept-empty",
        env = "SYNC_AGENT_ACCEPT_EMPTY"
    )]
    pub sync_agent_accept_empty: bool,
    #[structopt(
        help = "IPFS endpoint with access to the subgraph files",
        long = "--ipfs",
        env = "IPFS"
    )]
    pub ipfs: Url,
    #[structopt(help = "Fisherman endpoint", long = "--fisherman", env = "FISHERMAN")]
    pub fisherman: Option<Url>,
    #[structopt(
        help = "Ethereum provider URLs, format: '<network>=<url>,...'\ne.g. rinkeby=eth-rinkeby.alchemyapi.io/v2/<api-key>",
        long = "--ethereum-providers",
        env = "ETHEREUM_PROVIDERS"
    )]
    pub ethereum_providers: EthereumProviders,
    #[structopt(
        help = "Network subgraph URL",
        long = "--network-subgraph",
        env = "NETWORK_SUBGRAPH"
    )]
    pub network_subgraph: String,
    #[structopt(
        help = "Network subgraph auth token",
        long = "--network-subgraph-auth-token",
        env = "NETWORK_SUBGRAPH_AUTH_TOKEN"
    )]
    pub network_subgraph_auth_token: String,
    #[structopt(
        help = "MIP weights and addresses, format: '<weight>:<address>,...'\ne.g. 0.1:0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        long = "--mips",
        env = "MIPS",
        default_value = "0.2:"
    )]
    pub mips: MIPs,
    #[structopt(help = "Format log output as JSON", long = "--log-json")]
    pub log_json: bool,
    #[structopt(
        long = "--indexer-selection-retry-limit",
        env = "INDEXER_SELECTION_LIMIT",
        default_value = "5"
    )]
    pub indexer_selection_retry_limit: usize,
    #[structopt(
        long = "--block-cache-head",
        env = "BLOCK_CACHE_HEAD",
        default_value = "64"
    )]
    pub block_cache_head: usize,
    #[structopt(
        long = "--block-cache-size",
        env = "BLOCK_CACHE_SIZE",
        default_value = "32768"
    )]
    pub block_cache_size: usize,
    #[structopt(
        long = "--query-budget-scale",
        env = "QUERY_BUDGET_SCALE",
        default_value = "3.1"
    )]
    pub query_budget_scale: f64,
    #[structopt(
        long = "--query-budget-discount",
        env = "QUERY_BUDGET_DISCOUNT",
        default_value = "0.595"
    )]
    pub query_budget_discount: f64,
    #[structopt(
        help = "The number of processes per Gateway location. This is used when approximating worldwide query volume.",
        long = "--replica-count",
        env = "REPLICA_COUNT"
    )]
    pub replica_count: u64,
    #[structopt(
        help = "The number of geographic Gateway locations. This is used when approximating worldwide query volume.",
        long = "--location-count",
        env = "LOCATION_COUNT"
    )]
    pub location_count: u64,
    #[structopt(long = "--port", env = "PORT", default_value = "6700")]
    pub port: u16,
    #[structopt(long = "--metrics-port", env = "METRICS_PORT", default_value = "7300")]
    pub metrics_port: u16,
    #[structopt(
        help = "Duration of IP rate limiting window in seconds",
        long = "--ip-rate-limit-window",
        env = "IP_RATE_LIMIT_WINDOW",
        default_value = "10"
    )]
    pub ip_rate_limit_window_secs: u8,
    #[structopt(
        help = "IP rate limit per window",
        long = "--ip-rate-limit",
        env = "IP_RATE_LIMIT",
        default_value = "250"
    )]
    pub ip_rate_limit: u16,
    #[structopt(
        help = "Duration of API rate limiting window in seconds",
        long = "--api-rate-limit-window",
        env = "API_RATE_LIMIT_WINDOW",
        default_value = "10"
    )]
    pub api_rate_limit_window_secs: u8,
    #[structopt(
        help = "API rate limit per window",
        long = "--api-rate-limit",
        env = "API_RATE_LIMIT",
        default_value = "1000"
    )]
    pub api_rate_limit: u16,
    #[structopt(
        help = "Stats database hostname",
        long = "--stats-db-host",
        env = "STATS_DB_HOST",
        default_value = "localhost"
    )]
    pub stats_db_host: String,
    #[structopt(
        help = "Stats database port",
        long = "--stats-db-port",
        env = "STATS_DB_PORT",
        default_value = "5432"
    )]
    pub stats_db_port: u16,
    #[structopt(
        help = "Stats database name",
        long = "--stats-db-name",
        env = "STATS_DB_NAME",
        default_value = "dev"
    )]
    pub stats_db_name: String,
    #[structopt(
        help = "Stats database username",
        long = "--stats-db-user",
        env = "STATS_DB_USER",
        default_value = "dev"
    )]
    pub stats_db_user: String,
    #[structopt(
        help = "Stats database password",
        long = "--stats-db-password",
        env = "STATS_DB_PASSWORD",
        default_value = "dev"
    )]
    pub stats_db_password: String,
    #[structopt(
        help = "Redpanda broker domains",
        long = "--brokers",
        env = "REDPANDA_BROKERS"
    )]
    pub redpanda_brokers: String,
    #[structopt(
        help = "Security protocol",
        long = "--ssl-method",
        env = "REDPANDA_SECURITY_PROTOCOL"
    )]
    pub security_protocol: Option<String>,
    #[structopt(
        help = "SASL mechanism",
        long = "--sasl",
        env = "REDPANDA_SASL_MECHANISM",
        default_value = "SCRAM-SHA-256"
    )]
    pub sasl_mechanism: String,
    #[structopt(help = "SASL user", long = "--sasl-user", env = "REDPANDA_SASL_USER")]
    pub sasl_username: Option<String>,
    #[structopt(
        help = "SASL password",
        long = "--sasl-password",
        env = "REDPANDA_SASL_PASSWORD"
    )]
    pub sasl_password: Option<String>,
    #[structopt(help = "SSL ca location", long = "--ssl-ca", env = "REDPANDA_SSL_CA")]
    pub ssl_ca_location: Option<String>,
    #[structopt(
        help = "SSL cert location",
        long = "--ssl-cert",
        env = "REDPANDA_SSL_CERT"
    )]
    pub ssl_certificate_location: Option<String>,
    #[structopt(
        help = "SSL key location",
        long = "--ssl-key",
        env = "REDPANDA_SSL_KEY"
    )]
    pub ssl_key_location: Option<String>,
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
        config.set("sasl.mechanism", &self.sasl_mechanism);
        if let Some(security_protocol) = self.security_protocol.as_ref() {
            config.set("security.protocol", security_protocol);
        }
        if let Some(sasl_username) = self.sasl_username.as_ref() {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = self.sasl_password.as_ref() {
            config.set("sasl.passwrsasl_password", sasl_password);
        }
        if let Some(ssl_ca_location) = self.ssl_ca_location.as_ref() {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_certificate_location) = self.ssl_certificate_location.as_ref() {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ssl_key_location) = self.ssl_key_location.as_ref() {
            config.set("ssl.key.location", ssl_key_location);
        }
        config
    }
}

#[derive(Debug)]
pub struct SignerKey(pub SecretKey);

impl FromStr for SignerKey {
    type Err = Box<dyn Error>;
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
pub struct EthereumProviders(pub Vec<ethereum_client::Provider>);

impl FromStr for EthereumProviders {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err_usage = "networks syntax: <network>=<rest-url>(,<ws-url>)?;...";
        let providers = s.split(";").collect::<Vec<&str>>();
        if providers.is_empty() {
            return Err(err_usage.into());
        }
        providers
            .into_iter()
            .map(
                |provider| -> Result<ethereum_client::Provider, Box<dyn Error>> {
                    let kv: Vec<&str> = provider.splitn(3, "=").collect();
                    let urls: Vec<Url> = kv
                        .get(1)
                        .ok_or_else::<Box<dyn Error>, _>(|| "Expected URLs, found none".into())?
                        .split(",")
                        .map(Url::parse)
                        .collect::<Result<Vec<Url>, url::ParseError>>()?;
                    if (urls.len() < 1) || (urls.len() > 2) {
                        return Err(format!(
                            "Expected 1-2 URLS per provider, found {}",
                            urls.len()
                        )
                        .into());
                    }
                    let mut rest_url = None;
                    let mut websocket_url = None;
                    for url in urls {
                        if let Some(scheme) = match url.scheme() {
                            "http" | "https" => rest_url.replace(url).map(|_| "REST"),
                            "ws" | "wss" => websocket_url.replace(url).map(|_| "WebSocket"),
                            scheme => {
                                return Err(format!("URL scheme not supported: {}", scheme).into())
                            }
                        } {
                            return Err(format!(
                                "Multiple {} API URLs found for {}, expected 1",
                                scheme, provider
                            )
                            .into());
                        }
                    }
                    Ok(ethereum_client::Provider {
                        network: kv[0].to_string(),
                        rest_url: rest_url
                            .ok_or_else::<Box<dyn Error>, _>(|| "REST API URL not found".into())?,
                        websocket_url,
                    })
                },
            )
            .collect::<Result<Vec<ethereum_client::Provider>, Box<dyn Error>>>()
            .map(|providers| EthereumProviders(providers))
            .map_err(|err| format!("{}\n{}", err_usage, err).into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MIPs(pub HashMap<Address, NotNan<f64>>);

impl FromStr for MIPs {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fields: Vec<&str> = s.split(":").collect();
        if fields.len() != 2 {
            return Err(format!("Expected <weight>:<address>..., found {:?}", s));
        }
        let weight = fields[0]
            .parse::<NotNan<f64>>()
            .map_err(|_| format!("Expected <weight> (f64), found {:?}", fields[0]))?;
        let addresses = fields[1]
            .split(",")
            .filter(|s| *s != "")
            .map(|s| s.parse::<Address>().map_err(|_| s))
            .collect::<Result<Vec<Address>, &str>>()
            .map_err(|s| format!("Expected <address>, found {:?}", s))?;

        Ok(MIPs(
            addresses.into_iter().map(|addr| (addr, weight)).collect(),
        ))
    }
}
