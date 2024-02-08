use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};

use alloy_primitives::Address;
use eventuals::{Eventual, Ptr};
use thegraph::subscriptions::auth::{
    parse_auth_token as parse_bearer_token, verify_auth_token_claims, AuthTokenClaims,
};
use thegraph::types::{DeploymentId, SubgraphId};
use tokio::sync::RwLock;

use crate::client_query::query_settings::QuerySettings;
use crate::client_query::rate_limit_settings::RateLimitSettings;
use crate::subscriptions::Subscription;
use crate::topology::Deployment;

use super::common;

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

    /// Subscription query counters.
    pub(crate) query_counters: Arc<RwLock<HashMap<Address, AtomicUsize>>>,
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
    pub fn is_domain_allowed(&self, chain_id: u64, contract: &Address) -> bool {
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
) -> anyhow::Result<(
    AuthTokenClaims,
    Option<QuerySettings>,
    Option<RateLimitSettings>,
)> {
    let (claims, signature) =
        parse_bearer_token(token).map_err(|_| anyhow::anyhow!("invalid auth token"))?;

    // Verify the auth token signature
    if verify_auth_token_claims(&claims, &signature).is_err() {
        return Err(anyhow::anyhow!("invalid auth token signature"));
    }

    let user = claims.user();

    // Retrieve the subscription associated with the auth token user
    // TODO(LNSD): Pass the subscription around to support executing the requirements checks in the middleware
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

    Ok((claims, None, Some(rate_limit_settings)))
}

/// Check if the given deployment is authorized by the given API key.
pub fn is_deployment_authorized(auth_token: &AuthTokenClaims, deployment: &DeploymentId) -> bool {
    let allowed_deployments = &auth_token.allowed_deployments;
    common::is_deployment_authorized(allowed_deployments, deployment)
}

/// Check if the given subgraph is authorized by the given API key.
pub fn is_subgraph_authorized(auth_token: &AuthTokenClaims, subgraph: &SubgraphId) -> bool {
    let allowed_subgraphs = &auth_token.allowed_subgraphs;
    common::is_subgraph_authorized(allowed_subgraphs, subgraph)
}

/// Check if the given domain is authorized by the auth token claims.
pub fn is_domain_authorized(auth_token: &AuthTokenClaims, domain: &str) -> bool {
    // Get domain allowlist
    let allowed_domains: Vec<&str> = auth_token
        .allowed_domains
        .iter()
        .map(AsRef::as_ref)
        .collect();

    common::is_domain_authorized(&allowed_domains, domain)
}

pub async fn check_token(
    ctx: &AuthContext,
    auth_token: &AuthTokenClaims,
    rate_limit_settings: Option<RateLimitSettings>,
    deployments: &[Arc<Deployment>],
) -> anyhow::Result<()> {
    // Check deployment allowlist
    let allowed_deployments = &auth_token.allowed_deployments;
    if !common::are_deployments_authorized(allowed_deployments, deployments) {
        return Err(anyhow::anyhow!("Deployment not authorized by user"));
    }

    // Check subgraph allowlist
    let allowed_subgraphs = &auth_token.allowed_subgraphs;
    if !common::are_subgraphs_authorized(allowed_subgraphs, deployments) {
        return Err(anyhow::anyhow!("Subgraph not authorized by user"));
    }

    // This is safe, since we have already verified the signature and the claimed signer match in
    // the `parse_bearer_token` function. This is placed before the subscriptions domain check to
    // allow the same special query keys to be used across testnet & mainnet.
    if ctx.is_special_signer(&auth_token.signer) {
        return Ok(());
    }

    let chain_id = auth_token.chain_id();
    let contract = auth_token.contract();
    let signer = auth_token.signer();
    let user = auth_token.user();

    // If no active subscription is found, assume the user is not subscribed. And
    // provide a default subscription with 0 rate.
    let subscription = ctx
        .get_subscription_for_user(&user)
        .unwrap_or_else(|| Subscription {
            signers: vec![user],
            rate: 0,
        });

    let queries_per_minute = subscription
        .rate
        .checked_div(ctx.rate_per_query)
        .unwrap_or(0) as usize;
    tracing::debug!(
        subscription_payment_rate = subscription.rate,
        queries_per_minute,
    );
    if queries_per_minute == 0 {
        return Err(anyhow::anyhow!("Subscription not found for user {}", user));
    }

    // Check if the signer is authorized for the user.
    //
    // If no active subscription was found for the user, as we are returning
    // a default subscription that includes the user in the signers set, this
    // check will always pass.
    if (signer != user) && !subscription.signers.contains(&signer) {
        return Err(anyhow::anyhow!(
            "signer {signer} not authorized for user {user}"
        ));
    }

    // Check if the auth token chain id and contract are among
    // the allowed subscriptions domains.
    if !ctx.is_domain_allowed(chain_id, &contract) {
        return Err(anyhow::anyhow!(
            "query key chain_id or contract not allowed"
        ));
    }

    // TODO(LNSD): Move to rate limiter middleware
    if let Some(RateLimitSettings {
        key: user,
        queries_per_minute,
    }) = rate_limit_settings
    {
        // Check rate limit for subscriptions
        let counters = match ctx.query_counters.try_read() {
            Ok(counters) => counters,
            // Just skip if we can't acquire the read lock. This is a relaxed operation anyway.
            Err(_) => return Ok(()),
        };

        match counters.get(&user) {
            Some(counter) => {
                let count = counter.fetch_add(1, atomic::Ordering::Relaxed);
                // Note that counters are for 1 minute intervals.
                // 5720d5ea-cfc3-4862-865b-52b4508a4c14
                if count >= queries_per_minute {
                    return Err(anyhow::anyhow!("Rate limit exceeded"));
                }
            }
            // No entry, acquire write lock and insert.
            None => {
                drop(counters);
                let mut counters = ctx.query_counters.write().await;
                counters.insert(user, AtomicUsize::from(0));
            }
        }
    }

    Ok(())
}
