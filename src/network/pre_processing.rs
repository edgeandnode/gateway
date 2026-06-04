use std::collections::{HashMap, hash_map::Entry};

use anyhow::{anyhow, ensure};
use thegraph_core::{AllocationId, DeploymentId, IndexerId, SubgraphId};
use url::Url;

use crate::network::{
    indexer_processing::{IndexerRawInfo, IndexingRawInfo},
    subgraph_client,
    subgraph_client::types::SubgraphVersion,
    subgraph_processing::{
        AllocationInfo, DeploymentRawInfo, SubgraphRawInfo, SubgraphVersionRawInfo,
    },
};

pub fn into_internal_indexers_raw_info<'a>(
    data: impl Iterator<Item = &'a subgraph_client::types::Subgraph>,
) -> HashMap<IndexerId, IndexerRawInfo> {
    let mut indexers = HashMap::new();
    let mut largest_allocations = HashMap::new();

    for subgraph in data {
        for version in &subgraph.versions {
            process_allocations_into_indexers(
                &version.subgraph_deployment.allocations,
                version.subgraph_deployment.id,
                &mut indexers,
                &mut largest_allocations,
            );
        }
    }

    indexers
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
    // It is guaranteed that:
    // - All subgraphs have at least one version
    // - All versions are ordered by version number in descending order
    // See ref: 9936786a-e286-45f3-9190-8409d8389e88
    let subgraph_versions = subgraph
        .versions
        .into_iter()
        .filter_map(|version| {
            let mut raw_info = match into_subgraph_version_raw_info(version) {
                Ok(info) => info,
                Err(err) => {
                    tracing::debug!(subgraph = %subgraph.id, %err);
                    return None;
                }
            };
            raw_info.deployment.subgraphs.insert(subgraph.id);
            Some(raw_info)
        })
        .collect::<Vec<_>>();

    SubgraphRawInfo {
        id: subgraph.id,
        versions: subgraph_versions,
    }
}

/// Convert from the fetched subgraph version information into the internal representation.
fn into_subgraph_version_raw_info(
    version: SubgraphVersion,
) -> anyhow::Result<SubgraphVersionRawInfo> {
    let deployment = version.subgraph_deployment;

    let deployment_allocations = deployment
        .allocations
        .into_iter()
        .map(|allocation| AllocationInfo {
            indexer: allocation.indexer.id,
        })
        .collect::<Vec<_>>();

    let manifest = deployment
        .manifest
        .ok_or_else(|| anyhow!("missing manifest"))?;
    let manifest_network = manifest
        .network
        .ok_or_else(|| anyhow!("manifest missing network"))?;

    let version_deployment = DeploymentRawInfo {
        id: deployment.id,
        allocations: deployment_allocations,
        manifest_network,
        manifest_start_block: manifest.start_block,
        subgraphs: Default::default(),
    };

    Ok(SubgraphVersionRawInfo {
        deployment: version_deployment,
    })
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
    let indexer_url: Url = indexer_url
        .parse()
        .map_err(|err| anyhow!("invalid URL: {err}"))?;
    ensure!(
        indexer_url.scheme().starts_with("http"),
        "invalid URL: invalid scheme"
    );
    ensure!(indexer_url.host().is_some(), "invalid URL: missing host");
    // ref: df8e647b-1e6e-422a-8846-dc9ee7e0dcc2
    ensure!(!indexer_url.cannot_be_a_base(), "invalid URL: invalid base");

    Ok(IndexerRawInfo {
        id: indexer.id,
        url: indexer_url,
        staked_tokens: indexer.staked_tokens,
        indexings: Default::default(),
    })
}

/// Convert unpublished deployments (not linked to any active subgraph) into internal representation.
///
/// Unpublished deployments have `activeSubgraphCount: 0`. This function filters out deployments
/// without a valid manifest or without active allocations.
pub fn into_unpublished_deployments_raw_info(
    data: impl Iterator<Item = subgraph_client::types::SubgraphDeployment>,
) -> HashMap<DeploymentId, DeploymentRawInfo> {
    data.filter_map(|deployment| {
        // Validate manifest exists
        let manifest = deployment.manifest.as_ref()?;
        let network = manifest.network.as_ref()?;

        // Skip if no active allocations
        if deployment.allocations.is_empty() {
            return None;
        }

        let allocations = deployment
            .allocations
            .iter()
            .map(|a| AllocationInfo {
                indexer: a.indexer.id,
            })
            .collect();

        Some((
            deployment.id,
            DeploymentRawInfo {
                id: deployment.id,
                manifest_network: network.clone(),
                manifest_start_block: manifest.start_block,
                subgraphs: Default::default(), // Empty - unpublished
                allocations,
            },
        ))
    })
    .collect()
}

/// Extract indexer information from unpublished deployments.
pub fn into_indexers_raw_info_from_unpublished_deployments<'a>(
    data: impl Iterator<Item = &'a subgraph_client::types::SubgraphDeployment>,
) -> HashMap<IndexerId, IndexerRawInfo> {
    let mut indexers = HashMap::new();
    let mut largest_allocations = HashMap::new();

    for deployment in data {
        process_allocations_into_indexers(
            &deployment.allocations,
            deployment.id,
            &mut indexers,
            &mut largest_allocations,
        );
    }

    indexers
}

/// Process allocations and update indexer maps.
///
/// For each allocation, validates the indexer info and tracks the largest allocation
/// per (indexer, deployment) pair.
fn process_allocations_into_indexers(
    allocations: &[subgraph_client::types::Allocation],
    deployment_id: DeploymentId,
    indexers: &mut HashMap<IndexerId, IndexerRawInfo>,
    largest_allocations: &mut HashMap<(IndexerId, DeploymentId), (AllocationId, u128)>,
) {
    for allocation in allocations {
        let indexer_id = allocation.indexer.id;

        let indexer = match indexers.entry(indexer_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => match try_into_indexer_raw_info(&allocation.indexer) {
                Ok(info) => entry.insert(info),
                Err(err) => {
                    tracing::info!(
                        %deployment_id,
                        allocation_id = %allocation.id,
                        %indexer_id,
                        "invalid indexer info: {err}"
                    );
                    continue;
                }
            },
        };

        let indexing_largest_allocation =
            match largest_allocations.entry((indexer_id, deployment_id)) {
                Entry::Vacant(entry) => {
                    entry.insert((allocation.id, allocation.allocated_tokens));
                    allocation.id
                }
                Entry::Occupied(entry) => {
                    let (addr, amount) = entry.into_mut();
                    if allocation.allocated_tokens > *amount {
                        *addr = allocation.id;
                        *amount = allocation.allocated_tokens;
                    }
                    *addr
                }
            };

        indexer
            .indexings
            .entry(deployment_id)
            .or_insert(IndexingRawInfo {
                largest_allocation: allocation.id,
            })
            .largest_allocation = indexing_largest_allocation;
    }
}
