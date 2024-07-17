pub mod api_keys;
mod common;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, ensure};
use dashmap::DashMap;
use ordered_float::NotNan;
use thegraph_core::types::{alloy_primitives::Address, SubgraphId};
use tokio::{sync::watch, time::MissedTickBehavior};

use self::api_keys::APIKey;

#[derive(Clone, Debug, Default)]
pub struct AuthSettings {
    /// TODO: remove this field (should not be included in reporting)
    pub key: String,
    pub user: Address,
    pub authorized_subgraphs: Vec<SubgraphId>,
    pub budget_usd: Option<NotNan<f64>>,
}

impl AuthSettings {
    pub fn is_subgraph_authorized(&self, subgraph: &SubgraphId) -> bool {
        common::is_subgraph_authorized(&self.authorized_subgraphs, subgraph)
    }

    pub fn is_any_deployment_subgraph_authorized(&self, subgraphs: &[SubgraphId]) -> bool {
        subgraphs
            .iter()
            .any(|subgraph| self.is_subgraph_authorized(subgraph))
    }
}

#[derive(Clone)]
pub struct AuthContext {
    /// This is used to disable the payment requirement on testnets. If `true`, all queries will be
    /// checked for the `query_status` of their API key.
    pub payment_required: bool,
    pub api_keys: watch::Receiver<HashMap<String, APIKey>>,
    pub special_api_keys: Arc<HashSet<String>>,
    // TODO: remove after L1 deprecation
    pub rate_limiter: Option<RateLimiter>,
}

impl AuthContext {
    /// Parse an authorization token into its corresponding settings, and check that the query
    /// should be handled.
    pub fn check(&self, token: &str, domain: &str) -> anyhow::Result<AuthSettings> {
        ensure!(!token.is_empty(), "missing API key");

        if let Some(rate_limiter) = &self.rate_limiter {
            if rate_limiter.above_limit(token) {
                bail!("Rate limit exceeded. Querying L1 subgraphs is deprecated and will be removed soon.");
            }
            rate_limiter.increment(token);
        }

        // For now, the only option is an API key.
        api_keys::check(self, token, domain)
    }
}

// TODO: remove after L1 deprecation
#[derive(Clone)]
pub struct RateLimiter {
    counters: Arc<DashMap<String, u16>>,
    limit: u16,
}

impl RateLimiter {
    pub fn new(limit: u16) -> Self {
        let counters: Arc<DashMap<String, u16>> = Default::default();
        {
            let counters = counters.clone();
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tokio::spawn(async move {
                loop {
                    interval.tick().await;
                    counters.clear();
                }
            })
        };
        Self { counters, limit }
    }

    pub fn above_limit(&self, key: &str) -> bool {
        self.counters.get(key).map(|v| *v).unwrap_or(0) > self.limit
    }

    pub fn increment(&self, key: &str) {
        *self.counters.entry(key.to_string()).or_default() += 1;
    }
}
