pub mod api_keys;
mod common;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::ensure;
use ordered_float::NotNan;
use thegraph_core::types::{alloy_primitives::Address, SubgraphId};
use tokio::sync::watch;

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

    pub fn is_any_deployment_subgraph_authorized(&self, subgraphs: &[&SubgraphId]) -> bool {
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
}

impl AuthContext {
    /// Parse an authorization token into its corresponding settings, and check that the query
    /// should be handled.
    pub fn check(&self, token: &str, domain: &str) -> anyhow::Result<AuthSettings> {
        ensure!(!token.is_empty(), "missing bearer token");

        // For now, the only option is an API key.
        api_keys::check(self, token, domain)
    }
}
