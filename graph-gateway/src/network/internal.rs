use std::{collections::HashMap, time::Duration};

use alloy_primitives::Address;
use thegraph_core::types::{DeploymentId, SubgraphId};

use self::indexer_processing::IndexerRawInfo;
pub use self::{
    snapshot::{Indexer, Indexing, IndexingId, IndexingProgress, NetworkTopologySnapshot},
    subgraph_processing::{AllocationInfo, DeploymentInfo, SubgraphInfo, SubgraphVersionInfo},
};
use super::{
    config::VersionRequirements, indexer_addr_blocklist::AddrBlocklist,
    indexer_host_blocklist::HostBlocklist, indexer_host_resolver::HostResolver,
    indexer_indexing_cost_model_compiler::CostModelCompiler,
    indexer_indexing_cost_model_resolver::CostModelResolver,
    indexer_indexing_poi_blocklist::PoiBlocklist, indexer_indexing_poi_resolver::PoiResolver,
    indexer_indexing_progress_resolver::IndexingProgressResolver,
    indexer_version_resolver::VersionResolver, subgraph_client::Client as SubgraphClient,
    DeploymentError, SubgraphError,
};

mod indexer_processing;
mod pre_processing;
mod snapshot;
mod subgraph_processing;

/// Fetch the network topology information from the graph network subgraph.
pub async fn fetch_update(
    network: &PreprocessedNetworkInfo,
    state: &InternalState,
) -> NetworkTopologySnapshot {
    // Process network topology information
    let indexers_info = indexer_processing::process_info(state, &network.indexers).await;
    snapshot::new_from(
        indexers_info,
        network.subgraphs.clone(),
        network.deployments.clone(),
    )
}

pub struct PreprocessedNetworkInfo {
    subgraphs: HashMap<SubgraphId, Result<SubgraphInfo, SubgraphError>>,
    deployments: HashMap<DeploymentId, Result<DeploymentInfo, DeploymentError>>,
    indexers: HashMap<Address, IndexerRawInfo>,
}

pub struct InternalState {
    pub indexer_addr_blocklist: Option<AddrBlocklist>,
    pub indexer_host_resolver: HostResolver,
    pub indexer_host_blocklist: Option<HostBlocklist>,
    pub indexer_version_requirements: VersionRequirements,
    pub indexer_version_resolver: VersionResolver,
    pub indexer_indexing_pois_blocklist: Option<(PoiResolver, PoiBlocklist)>,
    pub indexer_indexing_progress_resolver: IndexingProgressResolver,
    pub indexer_indexing_cost_model_resolver: (CostModelResolver, CostModelCompiler),
}

/// Fetch the subgraphs information from the graph network subgraph and performs pre-processing
/// steps, i.e., validation and conversion into the internal representation.
///
///   1. Fetch the subgraphs information from the graph network subgraph.
///   2. Validate and convert the subgraphs fetched info into the internal representation.
///
/// If the fetch fails or the response is empty, an error is returned.
///
/// Invalid info is filtered out before converting into the internal representation.
pub async fn fetch_and_preprocess_subgraph_info(
    client: &SubgraphClient,
    timeout: Duration,
) -> anyhow::Result<PreprocessedNetworkInfo> {
    // Fetch the subgraphs information from the graph network subgraph
    let data = tokio::time::timeout(timeout, client.fetch()).await??;
    anyhow::ensure!(!data.is_empty(), "empty subgraph response");

    // Pre-process (validate and convert) the fetched subgraphs information
    let indexers = pre_processing::into_internal_indexers_raw_info(data.iter());
    let subgraphs = pre_processing::into_internal_subgraphs_raw_info(data.into_iter());
    let deployments = pre_processing::into_internal_deployments_raw_info(subgraphs.values());

    let subgraphs = subgraph_processing::process_subgraph_info(subgraphs);
    let deployments = subgraph_processing::process_deployments_info(deployments);

    Ok(PreprocessedNetworkInfo {
        subgraphs,
        deployments,
        indexers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    mod it_fetch_update;
    mod it_indexer_processing;
    mod tests_pre_processing;
    mod tests_subgraph_processing;
}
