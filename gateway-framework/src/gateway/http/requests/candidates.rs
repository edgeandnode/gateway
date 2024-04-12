use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use alloy_primitives::{Address, BlockNumber};
use gateway_common::types::Indexing;
use indexer_selection::{Candidate, Normalized};
use thegraph_core::types::DeploymentId;

use super::{budget::Budget, BlockRequirements, GatewayRequestContext};
use crate::{
    errors::{IndexerError, UnavailableReason},
    gateway::http::{GatewayImpl, GatewayState, IndexingStatus},
    network::indexing_performance::Snapshot,
    topology::network::Deployment,
};

pub fn available_indexers_for_deployments<G>(
    state: &GatewayState<G>,
    deployments: &[Arc<Deployment>],
    indexer_errors: &mut BTreeMap<Address, IndexerError>,
) -> BTreeSet<Indexing>
where
    G: GatewayImpl,
{
    let mut available_indexers: BTreeSet<Indexing> = deployments
        .iter()
        .flat_map(move |deployment| {
            let id = deployment.id;
            deployment.indexers.keys().map(move |indexer| Indexing {
                indexer: *indexer,
                deployment: id,
            })
        })
        .collect();

    let blocklist = state
        .indexings_blocklist
        .value_immediate()
        .unwrap_or_default();

    available_indexers.retain(|candidate| {
        if blocklist.contains(candidate) || state.bad_indexers.contains(&candidate.indexer) {
            indexer_errors.insert(
                candidate.indexer,
                IndexerError::Unavailable(UnavailableReason::NoStatus),
            );
            return false;
        }
        true
    });

    available_indexers
}

pub async fn prepare_candidate<G>(
    state: &GatewayState<G>,
    context: &GatewayRequestContext,
    request: &mut G::Request,
    statuses: &HashMap<Indexing, G::IndexingStatus>,
    performance_snapshots: &HashMap<Indexing, Snapshot>,
    block_requirements: &BlockRequirements,
    chain_head: BlockNumber,
    blocks_per_minute: u64,
    versions_behind: &BTreeMap<DeploymentId, u8>,
    indexing: Indexing,
    budget: &Budget,
) -> Result<Candidate, IndexerError>
where
    G: GatewayImpl,
{
    let info = state
        .network
        .indexing(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let status = statuses
        .get(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let performance = performance_snapshots
        .get(&indexing)
        .and_then(|snapshot| {
            performance(snapshot, block_requirements, chain_head, blocks_per_minute)
        })
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let fee = Normalized::new(
        (state
            .gateway_impl
            .indexer_request_fee(context, request, &status)
            .await?) as f64
            / budget.budget as f64,
    )
    .unwrap_or(Normalized::ONE);

    if let Some((min, max)) = &block_requirements.range {
        // Allow indexers if their last reported block is "close enough" to the required block
        // range. This is to compensate for the gateway's lack of knowledge about which blocks
        // indexers have responded with already. All else being equal, indexers closer to chain head
        // and with higher success rate will be favored.
        let range =
            status.min_block().unwrap_or(0)..=(performance.latest_block + blocks_per_minute);
        let number_gte = block_requirements.number_gte.unwrap_or(0);
        if !range.contains(min) || !range.contains(max) || (*range.end() < number_gte) {
            return Err(IndexerError::Unavailable(UnavailableReason::MissingBlock));
        }
    }

    Ok(Candidate {
        indexer: indexing.indexer,
        deployment: indexing.deployment,
        url: info.url.clone(),
        perf: performance.response,
        fee,
        seconds_behind: performance.seconds_behind,
        slashable_grt: (info.staked_tokens as f64 * 1e-18) as u64,
        subgraph_versions_behind: *versions_behind.get(&indexing.deployment).unwrap_or(&0),
        zero_allocation: info.allocated_tokens == 0,
    })
}

pub(crate) struct Perf {
    pub response: indexer_selection::ExpectedPerformance,
    pub latest_block: BlockNumber,
    pub seconds_behind: u32,
}

pub(crate) fn performance(
    snapshot: &Snapshot,
    block_requirements: &BlockRequirements,
    chain_head: BlockNumber,
    blocks_per_minute: u64,
) -> Option<Perf> {
    let latest_block = snapshot.latest_block?;
    let seconds_behind = if !block_requirements.latest || (blocks_per_minute == 0) {
        0
    } else {
        ((chain_head.saturating_sub(latest_block) as f64 / blocks_per_minute as f64) * 60.0).ceil()
            as u32
    };
    Some(Perf {
        response: snapshot.response.expected_performance(),
        latest_block,
        seconds_behind,
    })
}
