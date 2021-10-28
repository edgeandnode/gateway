use crate::{ethereum_client, indexer_selection::SecretKey, prelude::*};
use bip39;
use hdwallet::{self, KeyChain as _};
use std::error::Error;
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
    #[structopt(help = "Format log output as JSON", long = "--log-json")]
    pub log_json: bool,
    #[structopt(
        long = "--indexer-selection-retry-limit",
        env = "INDEXER_SELECTION_LIMIT",
        default_value = "5"
    )]
    pub indexer_selection_retry_limit: usize,
    #[structopt(
        long = "--query-budget",
        env = "QUERY_BUDGET",
        default_value = "0.0005"
    )]
    pub query_budget: GRT,
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
            .map(|provider| {
                let kv: Vec<&str> = provider.split("=").collect();
                if kv.len() != 2 {
                    return None;
                }
                let urls: Vec<Url> = kv[1]
                    .split(",")
                    .map(Url::parse)
                    .collect::<Result<Vec<Url>, url::ParseError>>()
                    .ok()?;
                if (urls.len() < 1) || (urls.len() > 2) {
                    return None;
                }
                let mut rest_url = None;
                let mut websocket_url = None;
                for url in urls {
                    match url.scheme() {
                        "http" | "https" => rest_url = Some(url),
                        "ws" | "wss" => websocket_url = Some(url),
                        _ => return None,
                    }
                }
                Some(ethereum_client::Provider {
                    network: kv[0].to_string(),
                    rest_url: rest_url?,
                    websocket_url,
                })
            })
            .collect::<Option<Vec<ethereum_client::Provider>>>()
            .map(|providers| EthereumProviders(providers))
            .ok_or::<String>(err_usage.into())
    }
}
