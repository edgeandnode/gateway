use std::path::PathBuf;

use alloy_primitives::Address;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use toolshed::url::Url;

use crate::config::{Chain, ExchangeRateProvider};

#[derive(Debug, Deserialize)]
pub struct AttestationConfig {
    pub chain_id: String,
    pub dispute_manager: Address,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct GatewayConfig {
    pub attestations: AttestationConfig,
    pub chains: Vec<Chain>,
    /// IPFS endpoint with access to the subgraph files
    #[serde_as(as = "DisplayFromStr")]
    pub ipfs: Url,
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
