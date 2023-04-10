use prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct SubgraphDeployments {
    pub inputs: Eventual<Ptr<Inputs>>,
}

#[derive(Clone)]
pub struct Inputs {
    // TODO: latest deployments may not be fully indexed, but the prior deployment might be.
    pub current_deployments: HashMap<SubgraphId, DeploymentId>,
    // A DeploymentId is the Qm hash representation of the Subgraph manifest uploaded to decentralized storage (currently IPFS).
    // A SubgraphId is a hash of the owning user address and an incrementing integer owned by the GNS contract.
    // It is possible that multiple users could create the same Subgraph manifest, and therefore get the same Qm hash DeploymentId.
    // And then these multiple users could publish the Subgraph.
    // This creates a scenario where a single DeploymentId could be linked with multiple SubgraphIDs.
    pub deployment_to_subgraphs: HashMap<DeploymentId, Vec<SubgraphId>>,
    // Set of deployments migrated away from the network environment served by this gateway.
    pub migrated_away: HashSet<DeploymentId>,
}

impl SubgraphDeployments {
    pub async fn current_deployment(&self, subgraph: &SubgraphId) -> Option<DeploymentId> {
        self.inputs
            .value()
            .await
            .ok()?
            .current_deployments
            .get(subgraph)
            .cloned()
    }

    pub async fn deployment_subgraphs(&self, deployment: &DeploymentId) -> Option<Vec<SubgraphId>> {
        self.inputs
            .value()
            .await
            .ok()?
            .deployment_to_subgraphs
            .get(deployment)
            .cloned()
    }

    pub async fn migrated_away(&self, deployment: &DeploymentId) -> bool {
        self.inputs
            .value()
            .await
            .map(|inputs| inputs.migrated_away.contains(deployment))
            .unwrap_or(false)
    }
}
