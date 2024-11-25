//! This module contains the [`PoiBlocklist`] struct, which is used to block indexers based
//! on their Proof of Indexing (POI) information.
//!
//! Given a list of blocked POIs, the blocklist checks if an indexer reports any of them as public
//! POIs. If a match is found, the indexer is blocked for the associated deployment ID.
//!
//! The blocklist caches the blocklist state for each indexer, so that subsequent checks against the
//! same indexer are fast. The cached entries are considered expired after a given TTL.

use std::collections::{HashMap, HashSet};

use thegraph_core::{BlockNumber, DeploymentId, ProofOfIndexing};

use crate::config::BlockedPoi;

/// A blocklist based on the Proof of Indexing (POI) of indexers.
#[derive(Default)]
pub struct PoiBlocklist {
    blocklist: HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>>,
}

impl PoiBlocklist {
    pub fn new(conf: Vec<BlockedPoi>) -> Self {
        let mut blocklist: HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>> =
            Default::default();
        for info in conf.into_iter() {
            blocklist
                .entry(info.deployment)
                .or_default()
                .push((info.block_number, info.public_poi.into()));
        }
        Self { blocklist }
    }

    pub fn is_empty(&self) -> bool {
        self.blocklist.is_empty()
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
            .flat_map(|deployment| {
                self.blocklist.get(deployment).into_iter().flat_map(|pois| {
                    pois.iter()
                        .map(|(block_number, _)| (*deployment, *block_number))
                })
            })
            .collect()
    }

    /// Return deployments with blocked POIs.
    pub fn check(
        &self,
        pois: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing>,
    ) -> HashSet<DeploymentId> {
        pois.iter()
            .filter(|((deployment_id, block_number), poi)| {
                self.check_poi(*deployment_id, *block_number, **poi)
            })
            .map(|((deployment_id, _), _)| *deployment_id)
            .collect()
    }

    /// Check if the POI is in the blocklist.
    fn check_poi(
        &self,
        deployment: DeploymentId,
        block_number: BlockNumber,
        poi: ProofOfIndexing,
    ) -> bool {
        match self.blocklist.get(&deployment) {
            None => false,
            Some(blocked_pois) => blocked_pois.contains(&(block_number, poi)),
        }
    }
}
