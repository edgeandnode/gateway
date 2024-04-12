use std::sync::Arc;

use anyhow::anyhow;

use crate::{
    auth::AuthToken,
    errors::Error,
    gateway::http::RequestSelector,
    topology::network::{Deployment, GraphNetwork, Subgraph},
};

pub fn resolve_and_authorize_deployments(
    network: &GraphNetwork,
    auth: &AuthToken,
    selector: &RequestSelector,
) -> Result<(Vec<Arc<Deployment>>, Option<Subgraph>), Error> {
    // Check if the query selector is authorized by the auth token
    match &selector {
        RequestSelector::Subgraph(id) => {
            if !auth.is_subgraph_authorized(id) {
                return Err(Error::Auth(anyhow!("Subgraph not authorized by user")));
            }
        }
        RequestSelector::Deployment(id) => {
            if !auth.is_deployment_authorized(id) {
                return Err(Error::Auth(anyhow!("Deployment not authorized by user")));
            }
        }
    }

    let (deployments, subgraph) = resolve_subgraph_deployments(network, &selector)?;
    tracing::info!(deployments = ?deployments.iter().map(|d| d.id).collect::<Vec<_>>());

    // Check authorization for the resolved deployments
    if !auth.are_deployments_authorized(&deployments) {
        return Err(Error::Auth(anyhow::anyhow!(
            "deployment not authorized by user"
        )));
    }

    if !auth.are_subgraphs_authorized(&deployments) {
        return Err(Error::Auth(anyhow::anyhow!(
            "subgraph not authorized by user"
        )));
    }

    Ok((deployments, subgraph))
}

/// Given a query selector, resolve the subgraph deployments for the query. If the selector is a subgraph ID, return
/// the subgraph's deployment instances. If the selector is a deployment ID, return the deployment instance.
fn resolve_subgraph_deployments(
    network: &GraphNetwork,
    selector: &RequestSelector,
) -> Result<(Vec<Arc<Deployment>>, Option<Subgraph>), Error> {
    match selector {
        RequestSelector::Subgraph(subgraph_id) => {
            // Get the subgraph by ID
            let subgraph = network
                .subgraph_by_id(subgraph_id)
                .ok_or_else(|| Error::SubgraphNotFound(anyhow!("{subgraph_id}")))?;

            // Get the subgraph's chain (from the last of its deployments)
            let subgraph_chain = subgraph
                .deployments
                .last()
                .map(|deployment| deployment.manifest.network.clone())
                .ok_or_else(|| Error::SubgraphNotFound(anyhow!("no matching deployments")))?;

            // Get the subgraph's deployments. Make sure we only select from deployments indexing
            // the same chain. This simplifies dealing with block constraints later
            let versions = subgraph
                .deployments
                .iter()
                .filter(|deployment| deployment.manifest.network == subgraph_chain)
                .cloned()
                .collect();

            Ok((versions, Some(subgraph)))
        }

        RequestSelector::Deployment(deployment_id) => {
            // Get the deployment by ID
            let deployment = network.deployment_by_id(deployment_id).ok_or_else(|| {
                Error::SubgraphNotFound(anyhow!("deployment not found: {deployment_id}"))
            })?;

            Ok((vec![deployment], None))
        }
    }
}
