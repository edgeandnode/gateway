use axum::extract::FromRef;
use thegraph::subscriptions::auth::{parse_auth_token, verify_auth_token_claims, AuthTokenClaims};

use super::AuthHandler;

/// App state (a.k.a [Context](crate::client_query::Context)) sub-state.
pub struct SubscriptionsAuthHandler {}

// TODO(LNSD): Use `client_query::Context` instead of `AuthHandler`.
impl FromRef<AuthHandler> for SubscriptionsAuthHandler {
    fn from_ref(_auth: &AuthHandler) -> Self {
        Self {}
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
