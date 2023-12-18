use std::collections::HashMap;

use alloy_primitives::Address;
use axum::extract::FromRef;
use eventuals::{Eventual, Ptr};
use thegraph::subscriptions::auth::{parse_auth_token, verify_auth_token_claims, AuthTokenClaims};

use crate::subscriptions::Subscription;

use super::AuthHandler;

/// App state (a.k.a [Context](crate::client_query::Context)) sub-state.
pub struct SubscriptionsAuthHandler {
    /// A map between the Subscription's user address and the actual
    /// active subscription.
    ///
    /// Subscriptions are fetched periodically (every 30s) from the Subscriptions subgraph by
    /// the gateway using the [`subscriptions_subgraph` client](crate::subscriptions_subgraph::Client).
    subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
}

// TODO(LNSD): Use `client_query::Context` instead of `AuthHandler`.
impl FromRef<AuthHandler> for SubscriptionsAuthHandler {
    fn from_ref(auth: &AuthHandler) -> Self {
        Self {
            subscriptions: auth.subscriptions.clone(),
        }
    }
}

impl SubscriptionsAuthHandler {
    /// Get the subscription associated with the auth token claims user.
    pub fn get_subscription(&self, user: &Address) -> Option<Subscription> {
        self.subscriptions.value_immediate()?.get(user).cloned()
    }
}

pub fn parse_bearer_token<S>(_state: &S, token: &str) -> anyhow::Result<AuthTokenClaims>
where
    SubscriptionsAuthHandler: FromRef<S>,
{
    let (claims, signature) =
        parse_auth_token(token).map_err(|_| anyhow::anyhow!("Invalid auth token"))?;

    // Verify the auth token signature
    if verify_auth_token_claims(&claims, &signature).is_err() {
        return Err(anyhow::anyhow!("Invalid auth token signature"));
    }

    Ok(claims)
}
