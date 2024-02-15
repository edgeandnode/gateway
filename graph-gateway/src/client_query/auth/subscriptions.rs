use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy_primitives::Address;
use eventuals::{Eventual, Ptr};
use thegraph::subscriptions::auth::{
    parse_auth_token as parse_bearer_token, verify_auth_token_claims, AuthTokenClaims,
};
use thegraph::types::{DeploymentId, SubgraphId};

use crate::client_query::query_settings::QuerySettings;
use crate::client_query::rate_limiter::RateLimitSettings;
use crate::subscriptions::Subscription;
use crate::topology::Deployment;

use super::common;

/// Auth token wrapper around the Subscriptions auth token claims and the subscription.
#[derive(Debug, Clone)]
pub struct AuthToken {
    /// The auth token claims.
    claims: AuthTokenClaims,
    /// The auth token subscription.
    subscription: Subscription,
}

impl AuthToken {
    /// Create a new auth token from the given auth token claims and subscription.
    pub fn new(claims: AuthTokenClaims, subscription: Subscription) -> Self {
        Self {
            claims,
            subscription,
        }
    }

    /// Get the auth token user address.
    pub fn user(&self) -> Address {
        self.claims.user()
    }

    /// Get the auth token claims.
    pub fn claims(&self) -> &AuthTokenClaims {
        &self.claims
    }

    /// Get the auth token subscription.
    pub fn subscription(&self) -> &Subscription {
        &self.subscription
    }

    /// Check if the given domain is authorized by the auth token claims.
    pub fn is_domain_authorized(&self, domain: &str) -> bool {
        let allowed_domains: Vec<&str> = self
            .claims
            .allowed_domains
            .iter()
            .map(AsRef::as_ref)
            .collect();

        common::is_domain_authorized(&allowed_domains, domain)
    }

    /// Check if the given subgraph is authorized by the auth token claims.
    pub fn is_subgraph_authorized(&self, subgraph: &SubgraphId) -> bool {
        let allowed_subgraphs = &self.claims.allowed_subgraphs;
        common::is_subgraph_authorized(allowed_subgraphs, subgraph)
    }

    /// Check if the given deployment is authorized by the auth token claims.
    pub fn is_deployment_authorized(&self, deployment: &DeploymentId) -> bool {
        let allowed_deployments = &self.claims.allowed_deployments;
        common::is_deployment_authorized(allowed_deployments, deployment)
    }

    pub fn are_subgraphs_authorized(&self, deployments: &[Arc<Deployment>]) -> bool {
        let allowed_subgraphs = &self.claims.allowed_subgraphs;
        common::are_subgraphs_authorized(allowed_subgraphs, deployments)
    }

    pub fn are_deployments_authorized(&self, deployments: &[Arc<Deployment>]) -> bool {
        let allowed_deployments = &self.claims.allowed_deployments;
        common::are_deployments_authorized(allowed_deployments, deployments)
    }
}

impl std::fmt::Display for AuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.claims.user())
    }
}

/// App state (a.k.a [Context](crate::client_query::Context)) sub-state.
pub struct AuthContext {
    /// A map between the Subscription's user address and the actual
    /// active subscription.
    ///
    /// Subscriptions are fetched periodically (every 30s) from the Subscriptions subgraph by
    /// the gateway using the [`subscriptions_subgraph` client](crate::subscriptions_subgraph::Client).
    pub(crate) subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,

    /// Auth token signers that don't require payment.
    pub(crate) special_signers: Arc<HashSet<Address>>,

    /// Subscription rate required per query per minute.
    pub(crate) rate_per_query: u128,

    /// A map between the chain id and the subscription contract address.
    pub(crate) subscription_domains: Arc<HashMap<u64, Address>>,
}

impl AuthContext {
    /// Get the subscription associated with the auth token claims user.
    pub fn get_subscription_for_user(&self, user: &Address) -> Option<Subscription> {
        self.subscriptions.value_immediate()?.get(user).cloned()
    }

    /// Returns `true` if the given address corresponds to a special signer.
    pub fn is_special_signer(&self, address: &Address) -> bool {
        self.special_signers.contains(address)
    }

    /// Whether the given chain id and contract address match the allowed subscription domains.
    pub fn is_subscription_domain_allowed(&self, chain_id: u64, contract: &Address) -> bool {
        self.subscription_domains
            .get(&chain_id)
            .map(|addr| addr == contract)
            .unwrap_or(false)
    }
}

/// Parse the bearer token as a Subscriptions auth token, verify the signature and return the
/// associated subscription and the expected queries per minute rate.
pub fn parse_auth_token(
    ctx: &AuthContext,
    token: &str,
) -> anyhow::Result<(AuthToken, Option<QuerySettings>, Option<RateLimitSettings>)> {
    let (claims, signature) =
        parse_bearer_token(token).map_err(|_| anyhow::anyhow!("invalid auth token"))?;

    // Verify the auth token signature
    if verify_auth_token_claims(&claims, &signature).is_err() {
        return Err(anyhow::anyhow!("invalid auth token signature"));
    }

    if ctx.is_special_signer(&claims.signer()) {
        let subscription = Subscription {
            signers: vec![claims.signer],
            rate: 0,
        };
        return Ok((AuthToken::new(claims, subscription), None, None));
    }

    let user = claims.user();

    // Retrieve the subscription associated with the auth token user
    let subscription = ctx
        .get_subscription_for_user(&user)
        .ok_or_else(|| anyhow::anyhow!("subscription not found for user {}", user))?;

    // Calculate the expected queries per minute rate
    let queries_per_minute = subscription
        .rate
        .checked_div(ctx.rate_per_query)
        .map(|rate| rate as usize)
        .unwrap_or(0);
    if queries_per_minute == 0 {
        tracing::warn!("subscription rate is too low for user {}", user);
        return Err(anyhow::anyhow!("subscription not found for user {}", user));
    }
    let rate_limit_settings = RateLimitSettings {
        key: user,
        queries_per_minute,
    };

    Ok((
        AuthToken::new(claims, subscription),
        None,
        Some(rate_limit_settings),
    ))
}

/// Perform subscriptions auth token specific requirements checks.
///
/// Checks performed:
///  1. Check if the auth token has been signed by a special signer.
///  2. Check if the auth token signer is authorized for the user.
///  3. Check if the auth token chain id and contract are among the allowed subscriptions domains.
pub fn check_auth_requirements(ctx: &AuthContext, token: &AuthToken) -> anyhow::Result<()> {
    let claims = token.claims();

    // This is safe, since we have already verified the signature and the claimed signer match in
    // the `parse_bearer_token` function. This is placed before the subscriptions domain check to
    // allow the same special query keys to be used across testnet & mainnet.
    if ctx.is_special_signer(&claims.signer()) {
        return Ok(());
    }

    // Check if the signer is authorized for the user
    //
    // If no active subscription was found for the user, as we are returning
    // a default subscription that includes the user in the signers set, this
    // check will always pass.
    let subscription = token.subscription();

    let signer = claims.signer();
    let user = claims.user();

    if (signer != user) && !subscription.signers.contains(&signer) {
        return Err(anyhow::anyhow!(
            "signer {signer} not authorized for user {user}"
        ));
    }

    // Check if the auth token chain id and contract are among
    // the allowed subscriptions domains
    if !ctx.is_subscription_domain_allowed(claims.chain_id(), &claims.contract()) {
        return Err(anyhow::anyhow!(
            "query key chain_id or contract not allowed"
        ));
    }

    Ok(())
}
