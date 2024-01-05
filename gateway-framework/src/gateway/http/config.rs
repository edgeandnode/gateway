use std::path::PathBuf;

use serde::Deserialize;

use crate::config::{Chain, ExchangeRateProvider};

#[derive(Debug, Deserialize)]
pub struct GatewayConfig {
    pub chains: Vec<Chain>,
    /// Ethereum RPC provider, or fixed exchange rate for testing
    pub exchange_rate_provider: ExchangeRateProvider,
    /// GeoIP database path
    pub geoip_database: Option<PathBuf>,
    /// GeoIP blocked countries (ISO 3166-1 alpha-2 codes)
    #[serde(default)]
    pub geoip_blocked_countries: Vec<String>,
}
