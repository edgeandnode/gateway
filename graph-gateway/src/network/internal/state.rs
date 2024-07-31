use crate::network::{
    config::VersionRequirements as IndexerVersionRequirements,
    indexer_addr_blocklist::AddrBlocklist, indexer_host_blocklist::HostBlocklist,
    indexer_host_resolver::HostResolver, indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_poi_blocklist::PoiBlocklist, indexer_indexing_poi_resolver::PoiResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::VersionResolver,
};

/// Internal type holding the network service state.
pub struct InternalState {
    pub indexer_addr_blocklist: Option<AddrBlocklist>,
    pub indexer_host_resolver: HostResolver,
    pub indexer_host_blocklist: Option<HostBlocklist>,
    pub indexer_version_requirements: IndexerVersionRequirements,
    pub indexer_version_resolver: VersionResolver,
    pub indexer_indexing_pois_blocklist: Option<(PoiResolver, PoiBlocklist)>,
    pub indexer_indexing_progress_resolver: IndexingProgressResolver,
    pub indexer_indexing_cost_model_resolver: (CostModelResolver, CostModelCompiler),
}
