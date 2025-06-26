use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, anyhow};
use serde::Deserialize;
use serde_json::json;
use tokio::time;
use tracing::{error, info, warn};

use crate::{
    indexer_client::{IndexerAuth, IndexerClient},
    network::subgraph_client::TrustedIndexer,
};

/// Horizon contract status tracker
#[allow(dead_code)] // Used when horizon contract detection is needed
///
/// This service monitors the network subgraph to detect when horizon contracts
/// are deployed and activated. This determines the TAP receipt generation strategy:
///
/// - Pre-horizon: Generate v1 receipts for all indexers
/// - Post-horizon: Generate v2 receipts only, process legacy v1 receipts
pub struct HorizonTracker {
    /// Whether horizon contracts are currently deployed and active
    horizon_active: Arc<AtomicBool>,
    /// Client for querying the network subgraph
    client: IndexerClient,
    /// Trusted indexers for network subgraph queries
    trusted_indexers: Vec<TrustedIndexer>,
    /// Check interval
    check_interval: Duration,
}

#[allow(dead_code)] // Used for horizon contract detection
#[derive(Debug, Deserialize)]
struct HorizonStatusQuery {
    data: Option<HorizonStatusData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[allow(dead_code)] // Used for horizon contract detection
#[derive(Debug, Deserialize)]
struct HorizonStatusData {
    /// Protocol information from the network subgraph
    #[serde(rename = "graphNetworks")]
    graph_networks: Vec<GraphNetwork>,
}

#[allow(dead_code)] // Used for horizon contract detection
#[derive(Debug, Deserialize)]
struct GraphNetwork {
    /// Network ID
    pub id: String,
    /// Current TAP protocol implementation contracts
    #[serde(rename = "tapCollectionContracts")]
    pub tap_collection_contracts: Vec<TapContract>,
    /// Legacy TAP allocation contracts (for v1)
    #[serde(rename = "tapAllocationContracts")]
    pub tap_allocation_contracts: Vec<TapContract>,
}

#[allow(dead_code)] // Used for horizon contract detection
#[derive(Debug, Deserialize)]
struct TapContract {
    /// Contract address
    pub id: String,
    /// Whether this contract is currently active
    pub active: bool,
    /// Contract deployment block
    #[serde(rename = "createdAtBlock")]
    pub created_at_block: Option<u64>,
}

#[allow(dead_code)] // Used when horizon contract detection is needed
impl HorizonTracker {
    pub fn new(
        client: IndexerClient,
        trusted_indexers: Vec<TrustedIndexer>,
        check_interval: Duration,
    ) -> Self {
        Self {
            horizon_active: Arc::new(AtomicBool::new(false)),
            client,
            trusted_indexers,
            check_interval,
        }
    }

    /// Check if horizon contracts are currently active
    pub fn is_horizon_active(&self) -> bool {
        self.horizon_active.load(Ordering::Relaxed)
    }

    /// Get the current TAP strategy based on horizon status
    pub fn get_tap_strategy(&self) -> TapStrategy {
        if self.is_horizon_active() {
            TapStrategy::PostHorizon
        } else {
            TapStrategy::PreHorizon
        }
    }

    /// Start the horizon monitoring background task
    pub async fn start_monitoring(self: Arc<Self>) {
        let mut interval = time::interval(self.check_interval);

        // Check immediately on startup
        if let Err(e) = self.check_horizon_status().await {
            error!(error = %e, "Initial horizon status check failed");
        }

        loop {
            interval.tick().await;

            if let Err(e) = self.check_horizon_status().await {
                warn!(error = %e, "Horizon status check failed, will retry");
            }
        }
    }

    /// Query the network subgraph to check horizon contract status
    async fn check_horizon_status(&self) -> anyhow::Result<()> {
        let query = r#"
            query {
                graphNetworks(first: 5) {
                    id
                    tapCollectionContracts(where: { active: true }) {
                        id
                        active
                        createdAtBlock
                    }
                    tapAllocationContracts(where: { active: true }) {
                        id
                        active
                        createdAtBlock
                    }
                }
            }"#;

        for indexer in &self.trusted_indexers {
            match self.query_indexer_for_horizon_status(indexer, query).await {
                Ok(is_active) => {
                    let was_active = self.horizon_active.load(Ordering::Relaxed);

                    if is_active != was_active {
                        if is_active {
                            info!(
                                "🚀 Horizon contracts detected as ACTIVE - switching to v2-only receipt generation"
                            );
                        } else {
                            info!(
                                "📡 Horizon contracts detected as INACTIVE - using v1 receipt generation"
                            );
                        }
                        self.horizon_active.store(is_active, Ordering::Relaxed);
                    }

                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        indexer = %indexer.url,
                        error = %e,
                        "Failed to check horizon status from indexer, trying next"
                    );
                    continue;
                }
            }
        }

        Err(anyhow!("All trusted indexers failed horizon status check"))
    }

    /// Query a specific indexer for horizon contract status
    async fn query_indexer_for_horizon_status(
        &self,
        indexer: &TrustedIndexer,
        query: &str,
    ) -> anyhow::Result<bool> {
        let query_body = json!({
            "query": query
        });

        let response = self
            .client
            .query_indexer(
                indexer.url.clone(),
                IndexerAuth::Free(&indexer.auth),
                &query_body.to_string(),
            )
            .await
            .context("Failed to query indexer")?;

        let horizon_response: HorizonStatusQuery = serde_json::from_str(&response.client_response)
            .context("Failed to parse horizon status response")?;

        if let Some(errors) = horizon_response.errors {
            return Err(anyhow!("GraphQL errors: {:?}", errors));
        }

        let data = horizon_response
            .data
            .ok_or_else(|| anyhow!("Missing data in response"))?;

        // Determine if horizon is active based on contract deployment
        let horizon_active = data.graph_networks.iter().any(|network| {
            // Horizon is active if there are active collection contracts (v2)
            // and fewer active allocation contracts (v1 being phased out)
            let active_collection_contracts = network
                .tap_collection_contracts
                .iter()
                .filter(|c| c.active)
                .count();

            let active_allocation_contracts = network
                .tap_allocation_contracts
                .iter()
                .filter(|c| c.active)
                .count();

            // Horizon is active if:
            // 1. There are active collection contracts (v2 support)
            // 2. Either no allocation contracts or collection contracts outnumber them
            active_collection_contracts > 0
                && (active_allocation_contracts == 0
                    || active_collection_contracts >= active_allocation_contracts)
        });

        info!(
            indexer = %indexer.url,
            horizon_active,
            "Horizon status check completed"
        );

        Ok(horizon_active)
    }
}

/// TAP strategy based on horizon contract deployment status
#[allow(dead_code)] // Used when horizon contracts need strategy selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapStrategy {
    /// Pre-horizon: Generate v1 receipts, serve all indexers
    PreHorizon,
    /// Post-horizon: Generate v2 receipts only, process legacy v1, serve v2-capable indexers only
    PostHorizon,
}

#[allow(dead_code)] // Used when horizon contract strategy is needed
impl TapStrategy {
    /// Should the gateway generate v1 receipts?
    pub fn should_generate_v1(&self) -> bool {
        matches!(self, TapStrategy::PreHorizon)
    }

    /// Should the gateway generate v2 receipts?
    pub fn should_generate_v2(&self) -> bool {
        matches!(self, TapStrategy::PostHorizon)
    }

    /// Can the gateway process v1 receipts?
    pub fn can_process_v1(&self) -> bool {
        true // Always can process v1 receipts for backwards compatibility
    }

    /// Can the gateway process v2 receipts?
    pub fn can_process_v2(&self) -> bool {
        true // Always can process v2 receipts
    }

    /// Get a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            TapStrategy::PreHorizon => "Pre-horizon: v1 generation, serve all indexers",
            TapStrategy::PostHorizon => {
                "Post-horizon: v2 generation only, serve v2-capable indexers"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tap_strategy() {
        let pre_horizon = TapStrategy::PreHorizon;
        assert!(pre_horizon.should_generate_v1());
        assert!(!pre_horizon.should_generate_v2());
        assert!(pre_horizon.can_process_v1());
        assert!(pre_horizon.can_process_v2());

        let post_horizon = TapStrategy::PostHorizon;
        assert!(!post_horizon.should_generate_v1());
        assert!(post_horizon.should_generate_v2());
        assert!(post_horizon.can_process_v1());
        assert!(post_horizon.can_process_v2());
    }
}
