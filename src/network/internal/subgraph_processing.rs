use std::collections::{HashMap, HashSet};

use thegraph_core::{alloy::primitives::BlockNumber, DeploymentId, IndexerId, SubgraphId};

use crate::network::errors::{DeploymentError, SubgraphError};

/// Internal representation of the fetched subgraph information.
///
/// This is not the final representation of the subgraph.
#[derive(Debug, Clone)]
pub(super) struct SubgraphRawInfo {
    pub id: SubgraphId,
    pub versions: Vec<SubgraphVersionRawInfo>,
}

/// Internal representation of the fetched subgraph version information.
///
/// This is not the final representation of the subgraph version.
#[derive(Debug, Clone)]
pub(super) struct SubgraphVersionRawInfo {
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
    // The indexer ID.
    pub indexer: IndexerId,
}

/// Process the fetched subgraphs' information.
///
/// - If the subgraph has no allocations, [`SubgraphError::NoAllocations`] is returned.
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
/// - If the deployment has no allocations, [`DeploymentError::NoAllocations`] is returned.
fn try_into_deployment_info(
    deployment: &DeploymentRawInfo,
) -> Result<DeploymentInfo, DeploymentError> {
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
