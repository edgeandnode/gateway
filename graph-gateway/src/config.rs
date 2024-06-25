//! The Graph Gateway configuration.

use custom_debug::CustomDebug;
use gateway_framework::config::{fmt_optional_url, GatewayConfig};
use semver::Version;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use url::Url;

use crate::indexers::public_poi::ProofOfIndexingInfo;

#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct Config {
    /// Configuration that is common across gateways
    pub common: GatewayConfig,

    /// Minimum graph-node version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_graph_node_version: Version,
    /// Minimum indexer-service version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_indexer_version: Version,
    /// POI blocklist
    pub poi_blocklist: Option<POIBlocklistConfig>,
    /// Subgraph studio admin auth token
    pub studio_auth: String,
    /// Subgraph studio admin url
    #[debug(with = fmt_optional_url)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub studio_url: Option<Url>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct POIBlocklistConfig {
    /// List of known bad POIs
    #[serde(default)]
    pub pois: Vec<ProofOfIndexingInfo>,
    /// POI blocklist update interval in minutes (default: 20 minutes)
    pub update_interval: Option<u64>,
}
