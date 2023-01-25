use eventuals::EventualExt;
use prelude::*;
use std::collections::HashMap;

#[derive(Clone)]
pub struct SubgraphDeployments {
    inputs: Eventual<Ptr<Inputs>>,
}

#[derive(Clone)]
struct Inputs {
    // TODO: latest deployments may not be fully indexed, but the prior deployment might be.
    current_deployments: HashMap<SubgraphID, SubgraphDeploymentID>,
    // A SubgraphDeploymentID is the Qm hash representation of the Subgraph manifest uploaded to decentralized storage (currently IPFS).
    // A SubgraphID is a hash of the owning user address and an incrementing integer owned by the GNS contract.
    // It is possible that multiple users could create the same Subgraph manifest, and therefore get the same Qm hash SubgraphDeploymentID.
    //  And then these multiple users could publish the Subgraph.
    // This creates a scenario where a single SubgraphDeploymentID could be linked with multiple SubgraphIDs.
    deployment_to_subgraphs: HashMap<SubgraphDeploymentID, Vec<SubgraphID>>,
}

impl SubgraphDeployments {
    pub fn new(
        subgraph_deployments: Eventual<(
            Ptr<Vec<(SubgraphID, SubgraphDeploymentID)>>,
            Ptr<Vec<(SubgraphDeploymentID, Vec<SubgraphID>)>>,
        )>,
    ) -> Self {
        let inputs = subgraph_deployments.map(
            |(current_deployments, deployment_to_subgraphs)| async move {
                let current_deployments = HashMap::from_iter(current_deployments.iter().cloned());
                let deployment_to_subgraphs =
                    HashMap::from_iter(deployment_to_subgraphs.iter().cloned());
                Ptr::new(Inputs {
                    current_deployments,
                    deployment_to_subgraphs,
                })
            },
        );
        Self { inputs }
    }

    pub async fn current_deployment(&self, subgraph: &SubgraphID) -> Option<SubgraphDeploymentID> {
        self.inputs
            .value()
            .await
            .ok()?
            .current_deployments
            .get(subgraph)
            .cloned()
    }

    pub async fn deployment_subgraph(
        &self,
        deployment: &SubgraphDeploymentID,
    ) -> Option<Vec<SubgraphID>> {
        self.inputs
            .value()
            .await
            .ok()?
            .deployment_to_subgraphs
            .get(deployment)
            .cloned()
    }
}
