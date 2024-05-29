use std::collections::{hash_map::Entry, HashMap};

use alloy_primitives::Address;
use anyhow::anyhow;
use thegraph_core::types::{DeploymentId, SubgraphId};
use url::Url;

use crate::network::{
    internal::{
        indexer_processing::{IndexerIndexingRawInfo, IndexerRawInfo},
        subgraph_processing::{DeploymentRawInfo, SubgraphRawInfo, SubgraphVersionRawInfo},
        AllocationInfo,
    },
    subgraph_client,
    subgraph_client::types::SubgraphVersion,
};

pub fn into_internal_indexers_raw_info<'a>(
    data: impl Iterator<Item = &'a subgraph_client::types::Subgraph>,
) -> HashMap<Address, IndexerRawInfo> {
    let mut indexer_indexing_largest_allocation: HashMap<(Address, DeploymentId), (Address, u128)> =
        HashMap::new();

    data.flat_map(|subgraph| {
        subgraph
            .versions
            .iter()
            .map(|version| (&subgraph.id, version))
    })
    .fold(HashMap::new(), |mut acc, (subgraph_id, version)| {
        for allocation in &version.subgraph_deployment.allocations {
            let indexer_id = allocation.indexer.id;
            let deployment_id = version.subgraph_deployment.id;

            // If the indexer info is not present, insert it if it is valid
            let indexer = match acc.entry(indexer_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => match try_into_indexer_raw_info(&allocation.indexer) {
                    Ok(info) => entry.insert(info),
                    Err(err) => {
                        // Log the error and skip the indexer
                        tracing::info!(
                            subgraph_id=%subgraph_id,
                            version=%version.version,
                            deployment_id=%deployment_id,
                            allocation_id=%allocation.id,
                            indexer_id=%indexer_id,
                            "invalid indexer info: {err}"
                        );
                        continue;
                    }
                },
            };

            // Update the indexer's indexings largest allocations table
            let indexing_largest_allocation = match indexer_indexing_largest_allocation
                .entry((indexer_id, deployment_id))
            {
                Entry::Vacant(entry) => {
                    entry.insert((allocation.id, allocation.allocated_tokens));
                    allocation.id
                }
                Entry::Occupied(entry) => {
                    let (largest_allocation_address, largest_allocation_amount) = entry.into_mut();
                    if allocation.allocated_tokens > *largest_allocation_amount {
                        *largest_allocation_address = allocation.id;
                        *largest_allocation_amount = allocation.allocated_tokens;
                    }
                    *largest_allocation_address
                }
            };

            // Update the indexer's indexings info
            let indexing =
                indexer
                    .indexings
                    .entry(deployment_id)
                    .or_insert(IndexerIndexingRawInfo {
                        largest_allocation: allocation.id,
                        total_allocated_tokens: 0,
                    });

            indexing.largest_allocation = indexing_largest_allocation;
            indexing.total_allocated_tokens = indexing
                .total_allocated_tokens
                .saturating_add(allocation.allocated_tokens);
        }

        acc
    })
}

/// Convert from the fetched subgraphs information into the internal representation.
///
/// If multiple subgraphs have the same ID, the first occurrence is kept and the rest are ignored.
pub fn into_internal_subgraphs_raw_info(
    data: impl Iterator<Item = subgraph_client::types::Subgraph>,
) -> HashMap<SubgraphId, SubgraphRawInfo> {
    data.into_iter()
        .fold(HashMap::new(), |mut acc, subgraph_data| {
            let subgraph_id = subgraph_data.id;

            let subgraph = match acc.entry(subgraph_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(into_subgraph_raw_info(subgraph_data));
                    return acc;
                }
            };

            // Add the subgraph version in the version-deployments
            for version in &mut subgraph.versions {
                version.deployment.subgraphs.insert(subgraph_id);
            }

            acc
        })
}

/// Convert from the fetched deployments information into the internal representation.
///
/// If multiple deployments have the same ID, allocations are merged.
pub fn into_internal_deployments_raw_info<'a>(
    data: impl Iterator<Item = &'a SubgraphRawInfo>,
) -> HashMap<DeploymentId, DeploymentRawInfo> {
    data.flat_map(|subgraph| subgraph.versions.iter().map(|version| &version.deployment))
        .fold(HashMap::new(), |mut acc, deployment_raw_info| {
            let deployment_id = deployment_raw_info.id;

            // If the deployment info is not present, insert it
            let deployment = match acc.entry(deployment_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(deployment_raw_info.clone());
                    return acc;
                }
            };

            // Merge the associated subgraphs
            deployment
                .subgraphs
                .extend(deployment_raw_info.subgraphs.iter().cloned());

            // Merge the associated allocations
            deployment
                .allocations
                .extend(deployment_raw_info.allocations.iter().cloned());

            acc
        })
}

/// Convert from the fetched subgraph information into the internal representation.
fn into_subgraph_raw_info(subgraph: subgraph_client::types::Subgraph) -> SubgraphRawInfo {
    let subgraph_id = subgraph.id;
    let subgraph_id_on_l2 = subgraph.id_on_l2;

    // It is guaranteed that:
    // - All subgraphs have at least one version
    // - All versions are ordered by version number in descending order
    // See ref: 9936786a-e286-45f3-9190-8409d8389e88
    let subgraph_versions = subgraph
        .versions
        .into_iter()
        .map(|version| {
            let mut raw_info = into_subgraph_version_raw_info(version);
            raw_info.deployment.subgraphs.insert(subgraph_id);
            raw_info
        })
        .collect::<Vec<_>>();

    SubgraphRawInfo {
        id: subgraph_id,
        id_on_l2: subgraph_id_on_l2,
        versions: subgraph_versions,
    }
}

/// Convert from the fetched subgraph version information into the internal representation.
fn into_subgraph_version_raw_info(version: SubgraphVersion) -> SubgraphVersionRawInfo {
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
        subgraphs: Default::default(),
        transferred_to_l2: deployment_transferred_to_l2,
    };

    SubgraphVersionRawInfo {
        version: version_number,
        deployment: version_deployment,
    }
}

/// Convert from the fetched indexer information into the internal representation.
///
/// If the indexer information is invalid, e.g., has no URL, an error is returned.
fn try_into_indexer_raw_info(
    indexer: &subgraph_client::types::Indexer,
) -> anyhow::Result<IndexerRawInfo> {
    // Check if the indexer is present
    let indexer_url = indexer.url.as_ref().ok_or_else(|| anyhow!("missing URL"))?;

    // Parse the URL. It must have an HTTP (or HTTPS) scheme and a valid host
    let indexer_url = indexer_url
        .parse::<Url>()
        .map_err(|err| anyhow!("invalid URL: parsing failed: {err}"))?;
    if !indexer_url.scheme().starts_with("http") {
        return Err(anyhow!("invalid URL: invalid scheme"));
    }
    if indexer_url.host().is_none() {
        return Err(anyhow!("invalid URL: missing host"));
    }

    Ok(IndexerRawInfo {
        id: indexer.id,
        url: indexer_url,
        staked_tokens: indexer.staked_tokens,
        indexings: Default::default(),
    })
}
