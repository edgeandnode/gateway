use std::{collections::HashMap, time::Duration};

use alloy_primitives::Address;
use anyhow::anyhow;
use thegraph_core::types::{DeploymentId, SubgraphId};

use self::{
    indexer_processing::IndexerRawInfo,
    subgraph_processing::{DeploymentRawInfo, SubgraphRawInfo},
};
pub use self::{
    snapshot::{Indexer, Indexing, IndexingId, IndexingProgress, NetworkTopologySnapshot},
    state::InternalState,
    subgraph_processing::{AllocationInfo, DeploymentInfo, SubgraphInfo, SubgraphVersionInfo},
};
use super::subgraph_client::Client as SubgraphClient;

mod indexer_processing;
mod pre_processing;
mod snapshot;
mod state;
mod subgraph_processing;

/// The network topology fetch timeout.
///
/// This timeout is applied independently to the indexers and subgraphs information fetches.
const NETWORK_TOPOLOGY_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

/// Fetch the network topology information from the graph network subgraph.
pub async fn fetch_update(
    client: &SubgraphClient,
    state: &InternalState,
) -> anyhow::Result<NetworkTopologySnapshot> {
    // Fetch and process the network topology information
    let (subgraphs_info, deployments_info, indexers_info) = match tokio::time::timeout(
        NETWORK_TOPOLOGY_FETCH_TIMEOUT,
        fetch_and_pre_process_subgraph_info(client),
    )
    .await
    {
        // If the fetch timed out, return an error
        Err(_) => Err(anyhow!("info fetch timed out")),
        Ok(resp) => match resp {
            // If the fetch failed, return an error
            Err(err) => Err(anyhow!("info fetch failed: {err}")),
            Ok(resp) => Ok(resp),
        },
    }?;

    // Process network topology information
    let indexers_info = indexer_processing::process_info(state, indexers_info).await;
    let subgraphs_info = subgraph_processing::process_subgraph_info(subgraphs_info);
    let deployments_info = subgraph_processing::process_deployments_info(deployments_info);

    Ok(snapshot::new_from(
        indexers_info,
        subgraphs_info,
        deployments_info,
    ))
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
async fn fetch_and_pre_process_subgraph_info(
    client: &SubgraphClient,
) -> anyhow::Result<(
    HashMap<SubgraphId, SubgraphRawInfo>,
    HashMap<DeploymentId, DeploymentRawInfo>,
    HashMap<Address, IndexerRawInfo>,
)> {
    // Fetch the subgraphs information from the graph network subgraph
    let data = client.fetch().await?;
    if data.is_empty() {
        return Err(anyhow!("empty subgraph fetch"));
    }

    // Pre-process (validate and convert) the fetched subgraphs information
    let indexers = pre_processing::into_internal_indexers_raw_info(data.iter());
    let subgraphs = pre_processing::into_internal_subgraphs_raw_info(data.into_iter());
    let deployments = pre_processing::into_internal_deployments_raw_info(subgraphs.values());

    Ok((subgraphs, deployments, indexers))
}

#[cfg(test)]
mod tests {
    use super::*;
    mod it_fetch_update;
    mod it_indexer_processing;
    mod tests_pre_processing;
    mod tests_subgraph_processing;
}
