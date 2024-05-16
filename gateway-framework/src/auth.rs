use ordered_float::NotNan;
use thegraph_core::types::SubgraphId;

pub use self::context::AuthContext;
use self::methods::{api_keys, subscriptions};

pub mod context;
pub mod methods;

/// User query settings typically associated with an auth token.
#[derive(Clone, Debug, Default)]
pub struct QuerySettings {
    pub budget_usd: Option<NotNan<f64>>,
}

#[derive(Clone, Debug)]
pub enum AuthToken {
    /// API key from the Subgraph Studio Database.
    ApiKey(Box<api_keys::AuthToken>),
    /// Auth token associated with a subscription.
    SubscriptionsAuthToken(Box<subscriptions::AuthToken>),
}

impl AuthToken {
    /// Check if the given subgraph is authorized for this auth token.
    pub fn is_subgraph_authorized(&self, subgraph: &SubgraphId) -> bool {
        match self {
            AuthToken::ApiKey(auth) => auth.is_subgraph_authorized(subgraph),
            AuthToken::SubscriptionsAuthToken(auth) => auth.is_subgraph_authorized(subgraph),
        }
    }

    /// Check if ANY of the given deployment subgraphs are authorized for this auth token.
    pub fn is_any_deployment_subgraph_authorized(&self, subgraphs: &[&SubgraphId]) -> bool {
        match self {
            AuthToken::ApiKey(auth) => subgraphs
                .iter()
                .any(|subgraph| auth.is_subgraph_authorized(subgraph)),
            AuthToken::SubscriptionsAuthToken(auth) => subgraphs
                .iter()
                .any(|subgraph| auth.is_subgraph_authorized(subgraph)),
        }
    }

    /// Check if the given origin domain is authorized for this auth token.
    pub fn is_domain_authorized(&self, domain: &str) -> bool {
        match self {
            AuthToken::ApiKey(auth) => auth.is_domain_authorized(domain),
            AuthToken::SubscriptionsAuthToken(auth) => auth.is_domain_authorized(domain),
        }
    }
}

impl From<api_keys::AuthToken> for AuthToken {
    fn from(auth: api_keys::AuthToken) -> Self {
        AuthToken::ApiKey(Box::new(auth))
    }
}

impl From<subscriptions::AuthToken> for AuthToken {
    fn from(auth: subscriptions::AuthToken) -> Self {
        AuthToken::SubscriptionsAuthToken(Box::new(auth))
    }
}
