use std::sync::Arc;

use thegraph::subscriptions::auth::AuthTokenClaims;
use thegraph::types::{DeploymentId, SubgraphId};

use crate::subgraph_studio::APIKey;

pub use self::common::QuerySettings;
pub use self::context::AuthContext;

mod common;
mod context;
mod studio;
mod subscriptions;

#[derive(Clone, Debug)]
pub enum AuthToken {
    /// API key from the Subgraph Studio Database.
    StudioApiKey(Arc<APIKey>),
    /// Auth token associated with a subscription.
    SubscriptionsAuthToken(AuthTokenClaims),
}

impl AuthToken {
    /// Check if the given subgraph is authorized for this auth token.
    pub fn is_subgraph_authorized(&self, subgraph: &SubgraphId) -> bool {
        match self {
            AuthToken::StudioApiKey(api_key) => studio::is_subgraph_authorized(api_key, subgraph),
            AuthToken::SubscriptionsAuthToken(claims) => {
                subscriptions::is_subgraph_authorized(claims, subgraph)
            }
        }
    }

    /// Check if the given deployment is authorized for this auth token.
    pub fn is_deployment_authorized(&self, deployment: &DeploymentId) -> bool {
        match self {
            AuthToken::StudioApiKey(api_key) => {
                studio::is_deployment_authorized(api_key, deployment)
            }
            AuthToken::SubscriptionsAuthToken(claims) => {
                subscriptions::is_deployment_authorized(claims, deployment)
            }
        }
    }

    /// Check if the given origin domain is authorized for this auth token.    
    pub fn is_domain_authorized(&self, domain: &str) -> bool {
        match self {
            AuthToken::StudioApiKey(api_key) => studio::is_domain_authorized(api_key, domain),
            AuthToken::SubscriptionsAuthToken(claims) => {
                subscriptions::is_domain_authorized(claims, domain)
            }
        }
    }

    /// Get the user settings associated with the auth token.
    ///
    /// See [`QuerySettings`] for more information.
    pub fn query_settings(&self) -> QuerySettings {
        match self {
            AuthToken::StudioApiKey(api_key) => studio::get_query_settings(api_key),
            AuthToken::SubscriptionsAuthToken(token_claims) => {
                subscriptions::get_query_settings(token_claims)
            }
        }
    }
}
