use tokio::sync::Mutex;

use super::indexer_processing::VersionRequirements as IndexerVersionRequirements;
use crate::network::{
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
    pub indexer_indexing_pois_blocklist: Option<(PoiBlocklist, PoiResolver)>,
    pub indexer_indexing_progress_resolver: IndexingProgressResolver,
    pub indexer_indexing_cost_model_resolver: (CostModelResolver, Mutex<CostModelCompiler>),
}

impl AsRef<IndexerVersionRequirements> for InternalState {
    fn as_ref(&self) -> &IndexerVersionRequirements {
        &self.indexer_version_requirements
    }
}

impl AsRef<Option<AddrBlocklist>> for InternalState {
    fn as_ref(&self) -> &Option<AddrBlocklist> {
        &self.indexer_addr_blocklist
    }
}

impl AsRef<HostResolver> for InternalState {
    fn as_ref(&self) -> &HostResolver {
        &self.indexer_host_resolver
    }
}

impl AsRef<Option<HostBlocklist>> for InternalState {
    fn as_ref(&self) -> &Option<HostBlocklist> {
        &self.indexer_host_blocklist
    }
}

impl AsRef<VersionResolver> for InternalState {
    fn as_ref(&self) -> &VersionResolver {
        &self.indexer_version_resolver
    }
}

impl AsRef<Option<(PoiBlocklist, PoiResolver)>> for InternalState {
    fn as_ref(&self) -> &Option<(PoiBlocklist, PoiResolver)> {
        &self.indexer_indexing_pois_blocklist
    }
}

impl AsRef<IndexingProgressResolver> for InternalState {
    fn as_ref(&self) -> &IndexingProgressResolver {
        &self.indexer_indexing_progress_resolver
    }
}

impl AsRef<(CostModelResolver, Mutex<CostModelCompiler>)> for InternalState {
    fn as_ref(&self) -> &(CostModelResolver, Mutex<CostModelCompiler>) {
        &self.indexer_indexing_cost_model_resolver
    }
}
