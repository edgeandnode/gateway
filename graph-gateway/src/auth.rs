use std::collections::{HashMap, HashSet};
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{bail, ensure};
use eventuals::{Eventual, EventualExt, Ptr};
use graph_subscriptions::subscription_tier::SubscriptionTiers;
use thegraph::subscriptions::auth::AuthTokenClaims;
use thegraph::types::{DeploymentId, SubgraphId};
use tokio::sync::RwLock;

use prelude::USD;

use crate::subgraph_studio::{APIKey, QueryStatus};
use crate::subscriptions::Subscription;
use crate::topology::Deployment;

use self::common::{are_deployments_authorized, are_subgraphs_authorized, is_domain_authorized};

mod common;
mod studio;
mod subscriptions;

pub struct AuthHandler {
    pub api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    pub special_api_keys: HashSet<String>,
    pub special_query_key_signers: HashSet<Address>,
    pub api_key_payment_required: bool,
    pub subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
    pub subscription_tiers: &'static SubscriptionTiers,
    pub subscription_domains: HashMap<u64, Address>,
    pub subscription_query_counters: RwLock<HashMap<Address, AtomicUsize>>,
}

#[derive(Debug)]
pub struct UserSettings {
    pub budget: Option<USD>,
}

pub enum AuthToken {
    /// API key from the Subgraph Studio Database.
    StudioApiKey(Arc<APIKey>),
    /// Auth token associated with a subscription.
    SubscriptionsAuthToken(AuthTokenClaims),
}

impl AuthHandler {
    pub fn create(
        api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
        special_api_keys: HashSet<String>,
        special_query_key_signers: HashSet<Address>,
        api_key_payment_required: bool,
        subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
        subscription_tiers: &'static SubscriptionTiers,
        subscription_domains: HashMap<u64, Address>,
    ) -> &'static Self {
        let handler: &'static Self = Box::leak(Box::new(Self {
            api_keys,
            special_api_keys,
            special_query_key_signers,
            api_key_payment_required,
            subscriptions,
            subscription_tiers,
            subscription_domains,
            subscription_query_counters: RwLock::default(),
        }));

        // Reset counters every minute.
        // 5720d5ea-cfc3-4862-865b-52b4508a4c14
        eventuals::timer(Duration::from_secs(60))
            .pipe_async(|_| async {
                let mut counters = handler.subscription_query_counters.write().await;
                counters.retain(|_, v| {
                    if v.load(atomic::Ordering::Relaxed) == 0 {
                        return false;
                    }
                    v.store(0, atomic::Ordering::Relaxed);
                    true
                });
            })
            .forever();

        handler
    }

    pub fn parse_bearer_token(&self, input: &str) -> anyhow::Result<AuthToken> {
        // Ensure the bearer token is not empty
        if input.is_empty() {
            return Err(anyhow::anyhow!("Not found"));
        }

        // First, parse the bearer token as it was a Studio API key
        if let Ok(api_key) = studio::parse_bearer_token(self, input) {
            return Ok(AuthToken::StudioApiKey(api_key));
        }

        // Otherwise, parse the bearer token as a Subscriptions auth token
        if let Ok(claims) = subscriptions::parse_bearer_token(self, input) {
            return Ok(AuthToken::SubscriptionsAuthToken(claims));
        }

        Err(anyhow::anyhow!("Invalid auth token"))
    }

    pub async fn check_token(
        &self,
        token: &AuthToken,
        deployments: &[Arc<Deployment>],
        domain: &str,
    ) -> anyhow::Result<()> {
        // Enforce the API key payment status, unless it's being subsidized.
        if let AuthToken::StudioApiKey(api_key) = &token {
            if self.api_key_payment_required
                && !api_key.is_subsidized
                && !self.special_api_keys.contains(&api_key.key)
            {
                match api_key.query_status {
                    QueryStatus::Active => (),
                    QueryStatus::Inactive => bail!("Querying not activated yet; make sure to add some GRT to your balance in the studio"),
                    QueryStatus::ServiceShutoff => bail!("Payment required for subsequent requests for this API key"),
                };
            }
        }

        // Check deployment allowlist
        let allowed_deployments: Vec<DeploymentId> = match token {
            AuthToken::StudioApiKey(api_key) => api_key.deployments.clone(),
            AuthToken::SubscriptionsAuthToken(claims) => claims
                .allowed_deployments
                .iter()
                .flat_map(|s| s.split(','))
                .filter_map(|s| s.parse::<DeploymentId>().ok())
                .collect(),
        };
        tracing::debug!(?allowed_deployments);

        if !are_deployments_authorized(&allowed_deployments, deployments) {
            return Err(anyhow::anyhow!("Deployment not authorized by user"));
        }

        // Check subgraph allowlist
        let allowed_subgraphs: Vec<SubgraphId> = match token {
            AuthToken::StudioApiKey(api_key) => api_key.subgraphs.clone(),
            AuthToken::SubscriptionsAuthToken(claims) => claims
                .allowed_subgraphs
                .iter()
                .flat_map(|s| s.split(','))
                .filter_map(|s| s.parse::<SubgraphId>().ok())
                .collect(),
        };
        tracing::debug!(?allowed_subgraphs);

        if !are_subgraphs_authorized(&allowed_subgraphs, deployments) {
            return Err(anyhow::anyhow!("Subgraph not authorized by user"));
        }

        // Check domain allowlist
        let allowed_domains: Vec<&str> = match token {
            AuthToken::StudioApiKey(api_key) => {
                api_key.domains.iter().map(|s| s.as_str()).collect()
            }
            AuthToken::SubscriptionsAuthToken(claims) => claims
                .allowed_domains
                .iter()
                .flat_map(|s| s.split(','))
                .collect(),
        };
        tracing::debug!(?allowed_domains);

        if !is_domain_authorized(&allowed_domains, domain) {
            return Err(anyhow::anyhow!("Domain not authorized by user"));
        }

        // Check rate limit for subscriptions. This step should be last to avoid invalid queries
        // taking up the rate limit.
        let auth_token = match token {
            AuthToken::StudioApiKey(_) => return Ok(()),
            AuthToken::SubscriptionsAuthToken(claims) => claims,
        };

        // This is safe, since we have already verified the signature and the claimed signer match.
        // This is placed before the subscriptions domain check to allow the same special query keys to be used across
        // testnet & mainnet.
        if self.special_query_key_signers.contains(&auth_token.signer) {
            return Ok(());
        }

        let user = auth_token.user();
        let subscription = self
            .subscriptions
            .value_immediate()
            .unwrap_or_default()
            .get(&user)
            .cloned()
            .unwrap_or_else(|| Subscription {
                signers: vec![user],
                rate: 0,
            });
        let subscription_tier = self.subscription_tiers.tier_for_rate(subscription.rate);
        tracing::debug!(
            subscription_payment_rate = subscription.rate,
            queries_per_minute = subscription_tier.queries_per_minute,
        );
        ensure!(
            subscription_tier.queries_per_minute > 0,
            "Subscription not found for user {user}"
        );

        let signer = auth_token.signer;
        ensure!(
            (signer == user) || subscription.signers.contains(&signer),
            "Signer {signer} not authorized for user {user}",
        );

        let matches_subscriptions_domain = self
            .subscription_domains
            .get(&auth_token.chain_id.into())
            .map(|contract| contract == &auth_token.contract)
            .unwrap_or(false);
        ensure!(
            matches_subscriptions_domain,
            "Query key chain_id or contract not allowed"
        );

        let user = auth_token.user();
        let counters = match self.subscription_query_counters.try_read() {
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
                ensure!(count < limit, "Rate limit exceeded");
            }
            // No entry, acquire write lock and insert.
            None => {
                drop(counters);
                let mut counters = self.subscription_query_counters.write().await;
                counters.insert(user, AtomicUsize::from(0));
            }
        }
        Ok(())
    }

    pub async fn query_settings(&self, token: &AuthToken) -> UserSettings {
        let budget = match token {
            AuthToken::StudioApiKey(api_key) => api_key.max_budget,
            _ => None,
        };
        UserSettings { budget }
    }
}
