use std::sync::Arc;

use thegraph::subscriptions::auth::AuthTokenClaims;

use crate::subgraph_studio::APIKey;

pub use self::context::{AuthContext, UserSettings};

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
    /// Check if the given origin domain is authorized for this auth token.    
    pub fn is_domain_authorized(&self, domain: &str) -> bool {
        match self {
            AuthToken::StudioApiKey(api_key) => studio::is_domain_authorized(api_key, domain),
            AuthToken::SubscriptionsAuthToken(claims) => {
                subscriptions::is_domain_authorized(claims, domain)
            }
        }
    }
}
