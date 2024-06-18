//! This module contains the [`PoiBlocklist`] struct, which is used to block indexers based
//! on their Proof of Indexing (POI) information.
//!
//! Given a list of blocked POIs, the blocklist checks if an indexer reports any of them as public
//! POIs. If a match is found, the indexer is blocked for the associated deployment ID.
//!
//! The blocklist caches the blocklist state for each indexer, so that subsequent checks against the
//! same indexer are fast. The cached entries are considered expired after a given TTL.

use std::collections::{HashMap, HashSet};

use alloy_primitives::BlockNumber;
use gateway_common::blocklist::Result as BlocklistResult;
use thegraph_core::types::{DeploymentId, ProofOfIndexing};

use crate::indexers::public_poi::ProofOfIndexingInfo;

/// A blocklist based on the Proof of Indexing (POI) of indexers.
pub struct PoiBlocklist {
    blocklist: HashMap<DeploymentId, HashSet<ProofOfIndexingInfo>>,
}

impl PoiBlocklist {
    /// Create a new indexer POI blocklist with the given configuration.
    pub fn new<TInfo>(conf: impl IntoIterator<Item = TInfo>) -> Self
    where
        TInfo: Into<ProofOfIndexingInfo>,
    {
        // Group the blocked POI info by deployment ID
        let mut conf_map = HashMap::new();
        for info in conf {
            let info = info.into();
            conf_map
                .entry(info.deployment_id)
                .or_insert_with(HashSet::new)
                .insert(info);
        }

        Self {
            blocklist: conf_map,
        }
    }

    /// Get a list of POIs metadata that are affected.
    ///
    /// If none of the deployments are affected, an empty list is returned. This allows to avoid
    /// querying the indexer for POIs if none of its deployments is affected.
    pub fn affected_pois_metadata<'a>(
        &self,
        deployments: impl IntoIterator<Item = &'a DeploymentId>,
    ) -> Vec<(DeploymentId, BlockNumber)> {
        deployments
            .into_iter()
            .flat_map(|deployment_id| {
                self.blocklist
                    .get(deployment_id)
                    .into_iter()
                    .flat_map(|pois| pois.iter().map(|poi_info| poi_info.meta()))
            })
            .collect()
    }

    /// Check if any of the reported POIs are in the blocklist.
    // TODO: Implement `Blocklist` trait
    pub fn check(
        &self,
        pois: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing>,
    ) -> HashMap<DeploymentId, BlocklistResult> {
        pois.iter()
            .map(|((deployment_id, block_number), poi)| {
                let state = self.check_poi(*deployment_id, *block_number, *poi);
                (*deployment_id, state)
            })
            .collect()
    }

    /// Check if the POI is in the blocklist.
    fn check_poi(
        &self,
        deployment_id: DeploymentId,
        block_number: BlockNumber,
        poi: ProofOfIndexing,
    ) -> BlocklistResult {
        match self.blocklist.get(&deployment_id) {
            None => BlocklistResult::Allowed,
            Some(blocked_pois) => {
                // Check if the POI is blocked
                if blocked_pois.iter().any(|blocked| {
                    blocked.deployment_id == deployment_id
                        && blocked.block_number == block_number
                        && blocked.proof_of_indexing == poi
                }) {
                    BlocklistResult::Blocked
                } else {
                    BlocklistResult::Allowed
                }
            }
        }
    }
}
