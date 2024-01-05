use std::path::PathBuf;

use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use toolshed::url::Url;

use crate::config::{Chain, ExchangeRateProvider};

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct GatewayConfig {
    pub chains: Vec<Chain>,
    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,
    /// Network subgraph query path
    #[serde_as(as = "DisplayFromStr")]
    pub network_subgraph: Url,
    /// L2 gateway to forward client queries to
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub l2_gateway: Option<Url>,
    /// GeoIP database path
    pub geoip_database: Option<PathBuf>,
    /// GeoIP blocked countries (ISO 3166-1 alpha-2 codes)
    #[serde(default)]
    pub geoip_blocked_countries: Vec<String>,
}
