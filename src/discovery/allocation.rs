use std::{collections::HashMap, sync::Arc};

use thegraph_core::types::AllocationId;

use crate::errors::UnavailableReason;

use super::{network_subgraph, Allocation, Indexer, IndexingStatus};

pub struct Resolver {
    indexing_status: HashMap<AllocationId, IndexingStatus>,
}

impl Resolver {
    pub fn new() -> Self {
        Self {
            indexing_status: Default::default(),
        }
    }

    pub async fn resolve(
        &self,
        indexer: &Arc<Indexer>,
        info: &[&network_subgraph::types::Allocation],
    ) -> Allocation {
        todo!()
    }
}

#[derive(Default)]
struct CostModelCache {
    // cache:
}
