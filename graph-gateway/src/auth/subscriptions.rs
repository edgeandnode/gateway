use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};

use alloy_primitives::Address;
use eventuals::{Eventual, Ptr};
use graph_subscriptions::subscription_tier::{SubscriptionTier, SubscriptionTiers};
use thegraph::subscriptions::auth::{parse_auth_token, verify_auth_token_claims, AuthTokenClaims};
use thegraph::types::{DeploymentId, SubgraphId};
use tokio::sync::RwLock;

use crate::subscriptions::Subscription;
use crate::topology::Deployment;

use super::common::{are_deployments_authorized, are_subgraphs_authorized, is_domain_authorized};

/// App state (a.k.a [Context](crate::client_query::Context)) sub-state.
pub struct AuthHandler {
    /// A map between the Subscription's user address and the actual
    /// active subscription.
    ///
    /// Subscriptions are fetched periodically (every 30s) from the Subscriptions subgraph by
    /// the gateway using the [`subscriptions_subgraph` client](crate::subscriptions_subgraph::Client).
    pub(super) subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,

    /// Auth token signers that don't require payment.
    pub(super) special_signers: Arc<HashSet<Address>>,

    /// The subscription tiers.
    // TODO(LNSD): In general 'static references are not a good idea.
    //             We should consider using an `Arc`.
    pub(super) tiers: &'static SubscriptionTiers,

    /// A map between the chain id and the subscription contract address.
    pub(super) subscription_domains: Arc<HashMap<u64, Address>>,

    /// Subscription query counters.
    pub(super) query_counters: Arc<RwLock<HashMap<Address, AtomicUsize>>>,
}

impl AuthHandler {
    /// Get the subscription associated with the auth token claims user.
    pub fn get_subscription_for_user(&self, user: &Address) -> Option<Subscription> {
        self.subscriptions.value_immediate()?.get(user).cloned()
    }

    /// Returns `true` if the given address corresponds to a special signer.
    pub fn is_special_signer(&self, address: &Address) -> bool {
        self.special_signers.contains(address)
    }

    /// Returns the subscription tiers for the rate.
    pub fn tier_for_rate(&self, rate: u128) -> SubscriptionTier {
        self.tiers.tier_for_rate(rate)
    }

    /// Whether the given chain id and contract address match the allowed subscription domains.
    pub fn is_domain_allowed(&self, chain_id: u64, contract: &Address) -> bool {
        self.subscription_domains
            .get(&chain_id)
            .map(|addr| addr == contract)
            .unwrap_or(false)
    }
}

pub fn parse_bearer_token(_auth: &AuthHandler, token: &str) -> anyhow::Result<AuthTokenClaims> {
    let (claims, signature) =
        parse_auth_token(token).map_err(|_| anyhow::anyhow!("Invalid auth token"))?;

    // Verify the auth token signature
    if verify_auth_token_claims(&claims, &signature).is_err() {
        return Err(anyhow::anyhow!("Invalid auth token signature"));
    }

    Ok(claims)
}

pub async fn check_token(
    auth: &AuthHandler,
    auth_token: &AuthTokenClaims,
    deployments: &[Arc<Deployment>],
    domain: &str,
) -> anyhow::Result<()> {
    // Check deployment allowlist
    let allowed_deployments: Vec<DeploymentId> = auth_token
        .allowed_deployments
        .iter()
        .flat_map(|s| s.split(','))
        .filter_map(|s| s.parse::<DeploymentId>().ok())
        .collect();
    tracing::debug!(?allowed_deployments);

    if !are_deployments_authorized(&allowed_deployments, deployments) {
        return Err(anyhow::anyhow!("Deployment not authorized by user"));
    }

    // Check subgraph allowlist
    let allowed_subgraphs: Vec<SubgraphId> = auth_token
        .allowed_subgraphs
        .iter()
        .flat_map(|s| s.split(','))
        .filter_map(|s| s.parse::<SubgraphId>().ok())
        .collect();
    tracing::debug!(?allowed_subgraphs);

    if !are_subgraphs_authorized(&allowed_subgraphs, deployments) {
        return Err(anyhow::anyhow!("Subgraph not authorized by user"));
    }

    // Check domain allowlist
    let allowed_domains: Vec<&str> = auth_token
        .allowed_domains
        .iter()
        .flat_map(|s| s.split(','))
        .collect();
    tracing::debug!(?allowed_domains);

    if !is_domain_authorized(&allowed_domains, domain) {
        return Err(anyhow::anyhow!("Domain not authorized by user"));
    }

    // This is safe, since we have already verified the signature and the claimed signer match in
    // the `parse_bearer_token` function. This is placed before the subscriptions domain check to
    // allow the same special query keys to be used across testnet & mainnet.
    if auth.is_special_signer(&auth_token.signer) {
        return Ok(());
    }

    let chain_id = auth_token.chain_id.id();
    let contract = auth_token.contract;
    let signer = auth_token.signer;
    let user = auth_token.user();

    // If no active subscription is found, assume the user is not subscribed. And
    // provide a default subscription with 0 rate.
    let subscription = auth
        .get_subscription_for_user(&user)
        .unwrap_or_else(|| Subscription {
            signers: vec![user],
            rate: 0,
        });

    let subscription_tier = auth.tier_for_rate(subscription.rate);
    tracing::debug!(
        subscription_payment_rate = subscription.rate,
        queries_per_minute = subscription_tier.queries_per_minute,
    );
    if subscription_tier.queries_per_minute == 0 {
        return Err(anyhow::anyhow!("Subscription not found for user {}", user));
    }

    // Check if the signer is authorized for the user.
    //
    // If no active subscription was found for the user, as we are returning
    // a default subscription that includes the user in the signers set, this
    // check will always pass.
    if (signer != user) && !subscription.signers.contains(&signer) {
        return Err(anyhow::anyhow!(
            "Signer {signer} not authorized for user {user}"
        ));
    }

    // Check if the if the auth token chain id and contract are among
    // the allowed subscriptions domains.
    if !auth.is_domain_allowed(chain_id, &contract) {
        return Err(anyhow::anyhow!(
            "Query key chain_id or contract not allowed"
        ));
    }

    // Check rate limit for subscriptions
    let counters = match auth.query_counters.try_read() {
        Ok(counters) => counters,
        // Just skip if we can't acquire the read lock. This is a relaxed operation anyway.
        Err(_) => return Ok(()),
    };

    match counters.get(&user) {
        Some(counter) => {
            let count = counter.fetch_add(1, atomic::Ordering::Relaxed);
            // Note that counters are for 1 minute intervals.
            // 5720d5ea-cfc3-4862-865b-52b4508a4c14
            let limit = subscription_tier.queries_per_minute as usize;

            // This error message should remain constant, since the graph-subscriptions-api
            // relies on it to track rate limited conditions.
            // TODO: Monthly limits should use the message "Monthly query limit exceeded"
            if count >= limit {
                return Err(anyhow::anyhow!("Rate limit exceeded"));
            }
        }
        // No entry, acquire write lock and insert.
        None => {
            drop(counters);
            let mut counters = auth.query_counters.write().await;
            counters.insert(user, AtomicUsize::from(0));
        }
    }

    Ok(())
}
