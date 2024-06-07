use std::sync::Arc;

use anyhow::anyhow;

use crate::{
    auth::AuthSettings,
    errors::Error,
    gateway::http::RequestSelector,
    topology::network::{Deployment, GraphNetwork, Subgraph},
};

/// Given a query selector, resolve the subgraph deployments for the query. If
/// the selector is a subgraph ID, return the subgraph's deployment instances.
/// If the selector is a deployment ID, return the deployment instance.
pub fn resolve_and_authorize_deployments(
    network: &GraphNetwork,
    auth: &AuthSettings,
    selector: &RequestSelector,
) -> Result<(Vec<Arc<Deployment>>, Option<Subgraph>), Error> {
    // Check if the query selector is authorized by the auth token
    if let RequestSelector::Subgraph(id) = &selector {
        // If the subgraph is not authorized, return an error.
        if !auth.is_subgraph_authorized(id) {
            return Err(Error::Auth(anyhow!("Subgraph not authorized by user")));
        }
    }

    // Authorization is based on the "authorized subgraphs" allowlist. We need
    // to resolve the subgraph deployments to check if any of the deployment's
    // subgraphs are authorized, otherwise return an error.
    let (deployments, subgraph) = resolve_subgraph_deployments(network, selector)?;

    // If none of the subgraphs are authorized, return an error.
    let subgraphs = deployments
        .iter()
        .flat_map(|d| d.subgraphs.iter())
        .collect::<Vec<_>>();
    if !auth.is_any_deployment_subgraph_authorized(&subgraphs) {
        return Err(Error::Auth(anyhow!("Deployment not authorized by user")));
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
