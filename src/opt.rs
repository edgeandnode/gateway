use crate::{ethereum_client, indexer_selection::SecretKey, prelude::*};
use bip39;
use hdwallet::{self, KeyChain as _};
use ordered_float::NotNan;
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
        long = "--query-budget",
        env = "QUERY_BUDGET",
        default_value = "0.0005"
    )]
    pub query_budget: USD,
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
        env = "REDPANDA_BROKERS",
        default_value = "0.rp-43eaea8.04f9121.byoc.vectorized.cloud:30714,1.rp-43eaea8.04f9121.byoc.vectorized.cloud:30714,2.rp-43eaea8.04f9121.byoc.vectorized.cloud:30714"
    )]
    pub redpanda_brokers: String,
    #[structopt(
        help = "Security protocol",
        long = "--ssl-method",
        env = "REDPANDA_SECURITY_PROTOCOL",
        default_value = "sasl_ssl",
        name = "security.protocol"
    )]
    pub security_protocol: String,
    #[structopt(
        help = "SSL key location",
        long = "--ssl-key",
        env = "REDPANDA_SSL_KEY",
        name = "ssl.key.location"
    )]
    pub ssl_key_location: String,
    #[structopt(
        help = "SSL cert location",
        long = "--ssl-cert",
        env = "REDPANDA_SSL_CERT",
        name = "ssl.certificate.location"
    )]
    pub ssl_certificate_location: String,
    #[structopt(
        help = "SSL ca location",
        long = "--ssl-ca",
        env = "REDPANDA_SSL_CA",
        name = "ssl.ca.location"
    )]
    pub ssl_ca_location: String,
    #[structopt(
        help = "SASL mechanism",
        long = "--sasl",
        env = "REDPANDA_SASL_MECHANISM",
        name = "sasl.mechanism",
        default_value = "SCRAM-SHA-256"
    )]
    pub sasl_mechanism: String,
    #[structopt(
        help = "SASL user",
        long = "--sasl-user",
        env = "REDPANDA_SASL_USER",
        name = "sasl.username"
    )]
    pub sasl_username: String,
    #[structopt(
        help = "SASL password",
        long = "--sasl-password",
        env = "REDPANDA_SASL_PASSWORD",
        name = "sasl.password"
    )]
    pub sasl_password: String,
    #[structopt(
        help = "Message encoding",
        long = "--encoding",
        env = "MESSAGE_ENCODING",
        default_value = "avro"
    )]
    pub message_encoding: String,
}

impl Opt {
    /// Map the input CLI arguments into tuple array that can be used to configure librdkafka
    pub fn to_kafka_config(&self) -> Vec<(&str, &str)> {
        let mut vec = vec![];
        vec.push(("sasl.password", &self.sasl_password[..]));
        vec.push(("sasl.password", &self.sasl_password[..]));
        vec.push(("sasl.username", &self.sasl_username[..]));
        vec.push(("sasl.mechanism", &self.sasl_mechanism[..]));
        vec.push(("ssl.ca.location", &self.ssl_ca_location[..]));
        vec.push((
            "ssl.certificate.location",
            &self.ssl_certificate_location[..],
        ));
        vec.push(("ssl.key.location", &self.ssl_key_location[..]));
        vec.push(("security.protocol", &self.security_protocol[..]));

        return vec;
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
