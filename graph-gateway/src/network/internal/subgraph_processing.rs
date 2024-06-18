use std::collections::{HashMap, HashSet};

use alloy_primitives::{Address, BlockNumber};
use thegraph_core::types::{DeploymentId, SubgraphId};

use crate::network::errors::{DeploymentError, SubgraphError};

/// Internal representation of the fetched subgraph information.
///
/// This is not the final representation of the subgraph.
#[derive(Debug, Clone)]
pub(super) struct SubgraphRawInfo {
    pub id: SubgraphId,
    pub id_on_l2: Option<SubgraphId>,
    pub versions: Vec<SubgraphVersionRawInfo>,
}

/// Internal representation of the fetched subgraph version information.
///
/// This is not the final representation of the subgraph version.
#[derive(Debug, Clone)]
pub(super) struct SubgraphVersionRawInfo {
    pub version: u32,
    pub deployment: DeploymentRawInfo,
}

/// Internal representation of the fetched deployment information.
///
/// This is not the final representation of the deployment.
#[derive(Debug, Clone)]
pub(super) struct DeploymentRawInfo {
    pub id: DeploymentId,
    pub manifest_network: String,
    pub manifest_start_block: BlockNumber,
    pub subgraphs: HashSet<SubgraphId>,
    pub transferred_to_l2: bool,
    pub allocations: Vec<AllocationInfo>,
}

/// Internal representation of the processed subgraph information.
///
/// This is not the final representation of the subgraph.
#[derive(Debug, Clone)]
pub struct SubgraphInfo {
    pub id: SubgraphId,
    pub versions: Vec<SubgraphVersionInfo>,
}

/// Internal representation of the processed subgraph version information.
///
/// This is not the final representation of the subgraph version.
#[derive(Debug, Clone)]
pub struct SubgraphVersionInfo {
    pub version: u32,
    pub deployment_id: DeploymentId,
    pub deployment: Result<DeploymentInfo, DeploymentError>,
}

/// Internal representation of the processed deployment information.
///
/// This is not the final representation of the deployment.
#[derive(Debug, Clone)]
pub struct DeploymentInfo {
    pub id: DeploymentId,
    pub allocations: Vec<AllocationInfo>,
    pub manifest_network: String,
    pub manifest_start_block: BlockNumber,
    pub subgraphs: HashSet<SubgraphId>,
}

/// Internal representation of the processed allocation information.
///
/// This is not the final representation of the allocation.
#[derive(Debug, Clone)]
pub struct AllocationInfo {
    // The allocation ID.
    pub id: Address,
    // The indexer ID.
    pub indexer: Address,
}

/// Process the fetched subgraphs' information.
///
/// - If the subgraph was transferred to L2 and has no allocations,
/// [`SubgraphError::TransferredToL2`] error is returned.
/// - If the subgraph has no allocations, [`SubgraphError::NoAllocations`] is returned.
#[allow(clippy::type_complexity)]
pub(super) fn process_subgraph_info(
    subgraphs: HashMap<SubgraphId, SubgraphRawInfo>,
) -> HashMap<SubgraphId, Result<SubgraphInfo, SubgraphError>> {
    subgraphs
        .into_iter()
        .map(|(id, subgraph)| {
            // If the subgraph has no versions, return an error
            // Subgraphs are guaranteed to have at least one version by the network subgraph
            // client query. As such, this is a safety check.
            // See ref: 9936786a-e286-45f3-9190-8409d8389e88
            if subgraph.versions.is_empty() {
                return (id, Err(SubgraphError::NoValidVersions));
            }

            // Check if the subgraph was transferred to L2
            if let Err(err) = check_subgraph_transferred_to_l2(&subgraph) {
                return (id, Err(err));
            }

            // Check if the subgraph has any allocations
            if let Err(err) = check_subgraph_has_allocations(&subgraph) {
                return (id, Err(err));
            }

            // It is guaranteed that:
            // - All subgraphs have at least one version
            // - All versions are ordered by version number in descending order
            // See ref: 9936786a-e286-45f3-9190-8409d8389e88
            let subgraph_versions = subgraph
                .versions
                .into_iter()
                .map(|version| SubgraphVersionInfo {
                    version: version.version,
                    deployment_id: version.deployment.id,
                    deployment: try_into_deployment_info(&version.deployment),
                })
                .collect::<Vec<_>>();
            if subgraph_versions
                .iter()
                .all(|version| version.deployment.is_err())
            {
                return (id, Err(SubgraphError::NoValidVersions));
            }

            (
                id,
                Ok(SubgraphInfo {
                    id: subgraph.id,
                    versions: subgraph_versions,
                }),
            )
        })
        .collect()
}

/// Check if the subgraph was transferred to L2.
///
/// A subgraph is considered to be transferred to L2 if all its versions-deployments
/// are transferred to L2 (i.e., `transferred_to_l2` is `true`) and have no allocations.
fn check_subgraph_transferred_to_l2(subgraph: &SubgraphRawInfo) -> Result<(), SubgraphError> {
    let transferred_to_l2 = subgraph.versions.iter().all(|version| {
        version.deployment.transferred_to_l2 && version.deployment.allocations.is_empty()
    });

    if transferred_to_l2 {
        Err(SubgraphError::TransferredToL2 {
            id_on_l2: subgraph.id_on_l2,
        })
    } else {
        Ok(())
    }
}

/// Check if the subgraph has any allocations.
///
/// A subgraph is considered to have allocations if at least one of its versions-deployments
/// has at least one allocation.
fn check_subgraph_has_allocations(subgraph: &SubgraphRawInfo) -> Result<(), SubgraphError> {
    let has_allocations = subgraph
        .versions
        .iter()
        .any(|version| !version.deployment.allocations.is_empty());

    if !has_allocations {
        Err(SubgraphError::NoAllocations)
    } else {
        Ok(())
    }
}

/// Process the fetched deployments' information.
///
/// - If the deployment was transferred to L2 and has no allocations,
/// [`DeploymentError::TransferredToL2`] error is returned.
/// - If the deployment has no allocations, [`DeploymentError::NoAllocations`] is returned.
pub(super) fn process_deployments_info(
    deployments: HashMap<DeploymentId, DeploymentRawInfo>,
) -> HashMap<DeploymentId, Result<DeploymentInfo, DeploymentError>> {
    deployments
        .into_iter()
        .map(|(id, deployment)| {
            let deployment = try_into_deployment_info(&deployment);
            (id, deployment)
        })
        .collect()
}

/// Try to convert the deployment raw information into processed deployment information.
///
/// - If the deployment was marked as transferred to L2 and has no allocations,
/// [`DeploymentError::TransferredToL2`] error is returned.
/// - If the deployment has no allocations, [`DeploymentError::NoAllocations`] is returned.
fn try_into_deployment_info(
    deployment: &DeploymentRawInfo,
) -> Result<DeploymentInfo, DeploymentError> {
    // Check if the deployment was transferred to L2
    if deployment.transferred_to_l2 && deployment.allocations.is_empty() {
        return Err(DeploymentError::TransferredToL2);
    }

    // Check if the deployment has any allocations
    if deployment.allocations.is_empty() {
        return Err(DeploymentError::NoAllocations);
    }

    Ok(DeploymentInfo {
        id: deployment.id,
        allocations: deployment.allocations.clone(),
        manifest_network: deployment.manifest_network.clone(),
        manifest_start_block: deployment.manifest_start_block,
        subgraphs: deployment.subgraphs.clone(),
    })
}
