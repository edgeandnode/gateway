//! The Graph Gateway configuration.

use custom_debug::CustomDebug;
use semver::Version;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use url::Url;

use gateway_framework::{
    config::fmt_optional_url,
    gateway::http::{AttestationConfig, GatewayConfig},
};

use crate::indexers::public_poi::ProofOfIndexingInfo;

#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct Config {
    pub common: GatewayConfig,

    pub attestations: AttestationConfig,

    /// Graph network environment identifier, inserted into Kafka messages
    pub graph_env_id: String,
    /// Rounds of indexer selection and queries to attempt. Note that indexer queries have a 20s
    /// timeout, so setting this to 5 for example would result in a 100s worst case response time
    /// for a client query.
    pub indexer_selection_retry_limit: usize,
    /// Minimum graph-node version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_graph_node_version: Version,
    /// Minimum indexer-service version that will receive queries
    #[serde_as(as = "DisplayFromStr")]
    pub min_indexer_version: Version,
    /// POI blocklist
    pub poi_blocklist: Option<POIBlocklistConfig>,
    /// public API port
    pub port_api: u16,
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
