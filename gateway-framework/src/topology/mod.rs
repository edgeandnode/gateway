use std::{collections::HashMap, sync::Arc};

use alloy_primitives::Address;
use eventuals::{Eventual, EventualExt, Ptr};
use thegraph_core::types::DeploymentId;

use self::network::Deployment;
use crate::{gateway::http::IndexingStatus, indexing::Indexing, scalar::ReceiptSigner};

pub mod network;

pub fn keep_allocations_up_to_date(
    receipt_signer: &'static ReceiptSigner,
    deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    indexing_statuses: Eventual<Ptr<HashMap<Indexing, impl IndexingStatus>>>,
) {
    eventuals::join((deployments.clone(), indexing_statuses.clone()))
        .pipe_async(move |(deployments, indexing_statuses)| async move {
            update_allocations(receipt_signer, &deployments, &indexing_statuses).await;
        })
        .forever();
}

async fn update_allocations(
    receipt_signer: &ReceiptSigner,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    indexing_statuses: &HashMap<Indexing, impl IndexingStatus>,
) {
    tracing::info!(
        deployments = deployments.len(),
        indexings = deployments
            .values()
            .map(|d| d.indexers.len())
            .sum::<usize>(),
        indexing_statuses = indexing_statuses.len(),
    );

    let mut allocations: HashMap<(Address, DeploymentId), Address> = HashMap::new();
    for (deployment, indexer) in deployments.values().flat_map(|deployment| {
        deployment
            .indexers
            .values()
            .map(|indexer| (deployment.as_ref(), indexer.as_ref()))
    }) {
        allocations.insert((indexer.id, deployment.id), indexer.largest_allocation);
    }
    receipt_signer.update_allocations(&allocations).await;
}
