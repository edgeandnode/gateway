use std::collections::HashSet;

use ipnetwork::IpNetwork;
use thegraph_core::types::IndexerId;

use crate::network::{
    config::VersionRequirements as IndexerVersionRequirements, indexer_host_resolver::HostResolver,
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_poi_blocklist::PoiBlocklist, indexer_indexing_poi_resolver::PoiResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::VersionResolver,
};

pub struct InternalState {
    pub indexer_addr_blocklist: HashSet<IndexerId>,
    pub indexer_host_resolver: HostResolver,
    pub indexer_host_blocklist: HashSet<IpNetwork>,
    pub indexer_version_requirements: IndexerVersionRequirements,
    pub indexer_version_resolver: VersionResolver,
    pub poi_blocklist: PoiBlocklist,
    pub poi_resolver: PoiResolver,
    pub indexing_progress_resolver: IndexingProgressResolver,
    pub cost_model_resolver: CostModelResolver,
    pub cost_model_compiler: CostModelCompiler,
}
