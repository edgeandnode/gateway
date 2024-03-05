use std::sync::Arc;

use thegraph_core::types::{DeploymentId, SubgraphId};

use crate::topology::Deployment;

pub use self::context::AuthContext;

mod common;
mod context;
pub mod studio;
pub mod subscriptions;

#[derive(Clone, Debug)]
pub enum AuthToken {
    /// API key from the Subgraph Studio Database.
    StudioApiKey(Box<studio::AuthToken>),
    /// Auth token associated with a subscription.
    SubscriptionsAuthToken(Box<subscriptions::AuthToken>),
}

impl AuthToken {
    /// Check if the given subgraph is authorized for this auth token.
    pub fn is_subgraph_authorized(&self, subgraph: &SubgraphId) -> bool {
        match self {
            AuthToken::StudioApiKey(auth) => auth.is_subgraph_authorized(subgraph),
            AuthToken::SubscriptionsAuthToken(auth) => auth.is_subgraph_authorized(subgraph),
        }
    }

    /// Check if the given deployment is authorized for this auth token.
    pub fn is_deployment_authorized(&self, deployment: &DeploymentId) -> bool {
        match self {
            AuthToken::StudioApiKey(auth) => auth.is_deployment_authorized(deployment),
            AuthToken::SubscriptionsAuthToken(auth) => auth.is_deployment_authorized(deployment),
        }
    }

    /// Check if the given origin domain is authorized for this auth token.    
    pub fn is_domain_authorized(&self, domain: &str) -> bool {
        match self {
            AuthToken::StudioApiKey(auth) => auth.is_domain_authorized(domain),
            AuthToken::SubscriptionsAuthToken(auth) => auth.is_domain_authorized(domain),
        }
    }

    pub fn are_subgraphs_authorized(&self, deployments: &[Arc<Deployment>]) -> bool {
        match self {
            AuthToken::StudioApiKey(auth) => auth.are_subgraphs_authorized(deployments),
            AuthToken::SubscriptionsAuthToken(auth) => auth.are_subgraphs_authorized(deployments),
        }
    }

    pub fn are_deployments_authorized(&self, deployments: &[Arc<Deployment>]) -> bool {
        match self {
            AuthToken::StudioApiKey(auth) => auth.are_deployments_authorized(deployments),
            AuthToken::SubscriptionsAuthToken(auth) => auth.are_deployments_authorized(deployments),
        }
    }
}

impl From<studio::AuthToken> for AuthToken {
    fn from(auth: studio::AuthToken) -> Self {
        AuthToken::StudioApiKey(Box::new(auth))
    }
}

impl From<subscriptions::AuthToken> for AuthToken {
    fn from(auth: subscriptions::AuthToken) -> Self {
        AuthToken::SubscriptionsAuthToken(Box::new(auth))
    }
}
