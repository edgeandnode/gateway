use eventuals::EventualExt;

use crate::prelude::*;
use std::collections::HashMap;

#[derive(Clone)]
pub struct SubgraphDeployments {
    inputs: Eventual<Ptr<Inputs>>,
}

#[derive(Clone)]
struct Inputs {
    current_deployments: HashMap<SubgraphID, SubgraphDeploymentID>,
    deployment_to_subgraph: HashMap<SubgraphDeploymentID, SubgraphID>,
}

impl SubgraphDeployments {
    pub fn new(
        subgraph_deployments: Eventual<Ptr<Vec<(SubgraphID, Vec<SubgraphDeploymentID>)>>>,
    ) -> Self {
        let inputs = subgraph_deployments.map(|deployments| async move {
            let current_deployments = deployments
                .iter()
                .filter_map(|(s, ds)| Some((s.clone(), ds.last()?.clone())))
                .collect();
            let deployment_to_subgraph = deployments
                .iter()
                .flat_map(|(s, ds)| ds.iter().map(move |d| (d.clone(), s.clone())))
                .collect();
            Ptr::new(Inputs {
                current_deployments,
                deployment_to_subgraph,
            })
        });
        Self { inputs }
    }

    pub fn current_deployment(&self, subgraph: &SubgraphID) -> Option<SubgraphDeploymentID> {
        self.inputs
            .value_immediate()?
            .current_deployments
            .get(subgraph)
            .cloned()
    }

    pub fn deployment_subgraph(&self, deployment: &SubgraphDeploymentID) -> Option<SubgraphID> {
        self.inputs
            .value_immediate()?
            .deployment_to_subgraph
            .get(deployment)
            .cloned()
    }
}
