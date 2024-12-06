use std::collections::BTreeMap;

use thegraph_core::alloy::primitives::Address;

use crate::{
    config::BlockedIndexer,
    network::{
        cost_model::CostModelResolver, host_filter::HostFilter,
        indexer_indexing_poi_blocklist::PoiBlocklist, indexer_indexing_poi_resolver::PoiResolver,
        indexer_indexing_progress_resolver::IndexingProgressResolver,
        version_filter::VersionFilter,
    },
};

pub struct InternalState {
    pub indexer_blocklist: BTreeMap<Address, BlockedIndexer>,
    pub indexer_host_filter: HostFilter,
    pub indexer_version_filter: VersionFilter,
    pub poi_blocklist: PoiBlocklist,
    pub poi_resolver: PoiResolver,
    pub indexing_progress_resolver: IndexingProgressResolver,
    pub cost_model_resolver: CostModelResolver,
}
