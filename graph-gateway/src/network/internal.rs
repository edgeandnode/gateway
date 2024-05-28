use std::{collections::HashMap, time::Duration};

use alloy_primitives::Address;
use anyhow::anyhow;
use itertools::Itertools;
use thegraph_core::types::SubgraphId;
use url::Url;

pub use self::{
    indexer_processing::{
        IndexerError, IndexerIndexingError, IndexerIndexingInfo, IndexerInfo, IndexingProgressInfo,
        VersionRequirements,
    },
    state::InternalState,
    subgraph_processing::{
        AllocationInfo, DeploymentError, DeploymentInfo, SubgraphError, SubgraphInfo,
        SubgraphVersionInfo,
    },
};
use self::{
    indexer_processing::{IndexerIndexingRawInfo, IndexerRawInfo},
    subgraph_processing::{DeploymentRawInfo, SubgraphRawInfo, SubgraphVersionRawInfo},
};
use super::{
    snapshot, snapshot::NetworkTopologySnapshot, subgraph, subgraph::Client as SubgraphClient,
};

mod indexer_processing;
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
    let (indexers_info, (subgraphs_info, deployments_info)) = futures::future::try_join(
        async {
            let info = match tokio::time::timeout(
                NETWORK_TOPOLOGY_FETCH_TIMEOUT,
                fetch_and_pre_process_indexers_info(client),
            )
            .await
            {
                // If the fetch timed out, return an error
                Err(_) => Err(anyhow!("indexers info fetch timed out")),
                Ok(resp) => match resp {
                    // If the fetch failed, return an error
                    Err(err) => Err(anyhow!("indexers info fetch failed: {err}")),
                    Ok(resp) => Ok(resp),
                },
            }?;

            // Process the fetched network topology information
            Ok::<_, anyhow::Error>(indexer_processing::process_info(state, info).await)
        },
        async {
            let info = match tokio::time::timeout(
                NETWORK_TOPOLOGY_FETCH_TIMEOUT,
                fetch_and_pre_process_subgraphs_info(client),
            )
            .await
            {
                // If the fetch timed out, return an error
                Err(_) => Err(anyhow!("subgraphs info fetch timed out")),
                Ok(resp) => match resp {
                    // If the fetch failed, return an error
                    Err(err) => Err(anyhow!("subgraphs info fetch failed: {err}")),
                    Ok(resp) => Ok(resp),
                },
            }?;

            // Process the fetched network topology information
            Ok::<_, anyhow::Error>(subgraph_processing::process_info(info))
        },
    )
    .await?;

    Ok(snapshot::new_from(
        indexers_info,
        subgraphs_info,
        deployments_info,
    ))
}

/// Fetch the indexers information from the graph network subgraph and performs pre-processing
/// steps, i.e., validation and conversion into the internal representation.
///
///   1. Fetch the indexers information from the graph network subgraph.
///   2. Validate and convert the indexers fetched info into the internal representation.
///
/// If the fetch fails or the response is empty, an error is returned.
///
/// Invalid info is filtered out before converting into the internal representation. If no valid
/// indexers are found, an error is returned.
async fn fetch_and_pre_process_indexers_info(
    client: &SubgraphClient,
) -> anyhow::Result<HashMap<Address, IndexerRawInfo>> {
    // Fetch the indexers information from the graph network subgraph
    let indexers = client
        .fetch_indexers()
        .await
        .map_err(|err| anyhow!("indexers fetch failed: {err}"))?;
    if indexers.is_empty() {
        return Err(anyhow!("empty indexers fetch"));
    }

    // Map the fetched indexers info into the internal representation
    // If no valid indexers are found, an error is returned.
    let indexers = indexers
        .into_iter()
        .filter_map(|indexer| {
            let _span =
                tracing::debug_span!("indexer pre-processing", indexer.id = %indexer.id).entered();
            tracing::trace!(
                indexer.allocations_count = %indexer.allocations.len(),
                indexer.url = ?indexer.url,
            );

            match try_into_internal_indexer_info(indexer) {
                Ok(indexer) => Some((indexer.id, indexer)),
                Err(err) => {
                    tracing::debug!("filtering-out indexer: {err}");
                    None
                }
            }
        })
        .collect::<HashMap<_, _>>();

    // If no valid indexers are found, return an error
    if indexers.is_empty() {
        Err(anyhow!("no valid indexers found"))
    } else {
        Ok(indexers)
    }
}

/// Fetch the subgraphs information from the graph network subgraph and performs pre-processing
/// steps, i.e., validation and conversion into the internal representation.
///
///   1. Fetch the subgraphs information from the graph network subgraph.
///   2. Validate and convert the subgraphs fetched info into the internal representation.
///
/// If the fetch fails or the response is empty, an error is returned.
///
/// Invalid info is filtered out before converting into the internal representation. If no valid
/// subgraphs are found, an error is returned.
async fn fetch_and_pre_process_subgraphs_info(
    client: &SubgraphClient,
) -> anyhow::Result<HashMap<SubgraphId, SubgraphRawInfo>> {
    // Fetch the subgraphs information from the graph network subgraph
    let subgraphs = client
        .fetch_subgraphs()
        .await
        .map_err(|err| anyhow!("subgraphs fetch failed: {err}"))?;
    if subgraphs.is_empty() {
        return Err(anyhow!("empty subgraphs fetch"));
    }

    // Map the fetched subgraphs info into the internal representation
    let subgraphs = subgraphs
        .into_iter()
        .map(|subgraph| (subgraph.id, into_internal_subgraph_raw_info(subgraph)))
        .collect();

    Ok(subgraphs)
}

/// Convert from the fetched subgraph information into the internal representation.
fn into_internal_subgraph_raw_info(
    subgraph: subgraph::types::fetch_subgraphs::Subgraph,
) -> SubgraphRawInfo {
    let versions = subgraph
        .versions
        .into_iter()
        .map(|version| {
            let deployment = version.subgraph_deployment;

            let deployment_allocations = deployment
                .allocations
                .into_iter()
                .map(|allocation| AllocationInfo {
                    id: allocation.id,
                    indexer: allocation.indexer.id,
                })
                .collect::<Vec<_>>();

            let deployment_id = deployment.id;
            let deployment_transferred_to_l2 = deployment.transferred_to_l2;

            let version_number = version.version;
            let version_deployment = DeploymentRawInfo {
                id: deployment_id,
                allocations: deployment_allocations,
                manifest_network: deployment.manifest.network,
                manifest_start_block: deployment.manifest.start_block,
                transferred_to_l2: deployment_transferred_to_l2,
            };

            SubgraphVersionRawInfo {
                version: version_number,
                deployment: version_deployment,
            }
        })
        .collect::<Vec<_>>();

    SubgraphRawInfo {
        id: subgraph.id,
        id_on_l2: subgraph.id_on_l2,
        versions,
    }
}

/// Convert from the fetched indexer information into the internal representation.
///
/// If the indexer is invalid, e.g., has no URL, an error is returned.
fn try_into_internal_indexer_info(
    indexer: subgraph::types::fetch_indexers::Indexer,
) -> anyhow::Result<IndexerRawInfo> {
    // Check if the indexer is present
    let indexer_url = indexer.url.ok_or_else(|| anyhow!("missing URL"))?;

    // Parse the URL. It must have an HTTP (or HTTPS) scheme and a valid host.
    // Filter out indexers with invalid URLs.
    let indexer_url = indexer_url
        .parse::<Url>()
        .map_err(|err| anyhow!("invalid URL: parsing failed: {err}"))?;
    if !indexer_url.scheme().starts_with("http") {
        return Err(anyhow!("invalid URL: invalid scheme"));
    }
    if indexer_url.host().is_none() {
        return Err(anyhow!("invalid URL: missing host"));
    }

    // Check if the indexer has any allocations
    if indexer.allocations.is_empty() {
        return Err(anyhow!("no allocations"));
    }

    // Get the list of unique deployment IDs the indexer is associated with.
    // NOTE: The indexer is guaranteed to have at least one allocation and one
    // deployment.
    // See ref: d260724b-a445-4842-964e-fb95062c119d
    let indexer_deployment_ids = indexer
        .allocations
        .iter()
        .map(|alloc| alloc.subgraph_deployment.id)
        .unique()
        .collect::<Vec<_>>();
    if indexer_deployment_ids.is_empty() {
        return Err(anyhow!("no deployments"));
    }

    // Create the indexings' information table for the indexer
    let indexer_indexings_info = indexer_deployment_ids
        .iter()
        .filter_map(|deployment_id| {
            // Get the largest allocation and the total amount of tokens allocated for each indexing
            // NOTE: The allocations are ordered by `allocatedTokens` in descending order, and
            // the largest allocation is the first one.
            // See ref: d260724b-a445-4842-964e-fb95062c119d
            let mut indexer_allocations_iter = indexer
                .allocations
                .iter()
                .filter(|alloc| alloc.subgraph_deployment.id == *deployment_id)
                .peekable();

            // To avoid cloning the iterator, as we are interested in the first element, we use a
            // "peekable" iterator to "peek" the next element of the iterator without consuming it.
            let largest_allocation = indexer_allocations_iter.peek().map(|alloc| alloc.id)?;

            // Calculate the total amount of tokens allocated for the deployment
            let total_allocated_tokens = indexer_allocations_iter
                .map(|alloc| alloc.allocated_tokens)
                .sum();

            Some((
                *deployment_id,
                IndexerIndexingRawInfo {
                    largest_allocation,
                    total_allocated_tokens,
                },
            ))
        })
        .collect::<HashMap<_, _>>();

    Ok(IndexerRawInfo {
        id: indexer.id,
        url: indexer_url,
        staked_tokens: indexer.staked_tokens,
        deployments: indexer_deployment_ids,
        indexings: indexer_indexings_info,
    })
}

#[cfg(test)]
mod tests {
    mod it_indexer_processing;
    mod it_subgraph_processing;
}
