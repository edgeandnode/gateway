use std::collections::{BTreeMap, HashMap};

use alloy_primitives::{Address, BlockNumber};
use indexer_selection::{Candidate, Normalized};
use thegraph_core::types::DeploymentId;

use super::{blocks::BlockRequirements, budget::Budget};
use crate::{
    errors::{IndexerError, MissingBlockError, UnavailableReason},
    gateway::http::gateway::IndexingStatus,
    indexing::Indexing,
    network::indexing_performance::Snapshot,
    topology::network::GraphNetwork,
};

#[allow(clippy::too_many_arguments)]
pub fn prepare_candidate<R, S: IndexingStatus>(
    network: &GraphNetwork,
    fee_fn: impl Fn(&R, &S) -> Result<u128, IndexerError>,
    statuses: &HashMap<Indexing, S>,
    perf_snapshots: &HashMap<(Address, DeploymentId), Snapshot>,
    versions_behind: &BTreeMap<DeploymentId, u8>,
    request: &R,
    block_requirements: &BlockRequirements,
    chain_head: BlockNumber,
    blocks_per_minute: u64,
    budget: &Budget,
    indexing: Indexing,
) -> Result<Candidate, IndexerError> {
    let info = network
        .indexing(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let status = statuses
        .get(&indexing)
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let perf = perf_snapshots
        .get(&(indexing.indexer, indexing.deployment))
        .and_then(|snapshot| {
            performance(snapshot, block_requirements, chain_head, blocks_per_minute)
        })
        .ok_or(IndexerError::Unavailable(UnavailableReason::NoStatus))?;
    let fee = Normalized::new((fee_fn(request, status)? as f64) / budget.budget as f64)
        .unwrap_or(Normalized::ONE);

    if let Some((min, max)) = &block_requirements.range {
        // Allow indexers if their last reported block is "close enough" to the required block
        // range. This is to compensate for the gateway's lack of knowledge about which blocks
        // indexers have responded with already. All else being equal, indexers closer to chain head
        // and with higher success rate will be favored.
        let latest_block = status.block().max(perf.latest_block + blocks_per_minute);
        let range = status.min_block().unwrap_or(0)..=latest_block;
        let number_gte = block_requirements.number_gte.unwrap_or(0);
        let missing_block = match range {
            range if !range.contains(min) => Some(*min),
            range if !range.contains(max) => Some(*max),
            range if *range.end() < number_gte => Some(number_gte),
            _ => None,
        };
        if let Some(missing) = missing_block {
            let (missing, latest) = (Some(missing), None);
            return Err(IndexerError::Unavailable(UnavailableReason::MissingBlock(
                MissingBlockError { missing, latest },
            )));
        }
    }

    Ok(Candidate {
        indexer: indexing.indexer,
        deployment: indexing.deployment,
        url: info.url.clone(),
        perf: perf.response,
        fee,
        seconds_behind: perf.seconds_behind,
        slashable_grt: (info.staked_tokens as f64 * 1e-18) as u64,
        versions_behind: *versions_behind.get(&indexing.deployment).unwrap_or(&0),
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
