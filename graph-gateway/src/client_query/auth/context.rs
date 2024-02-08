use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy_primitives::Address;
use axum::extract::FromRef;
use eventuals::{Eventual, Ptr};

use crate::client_query::query_settings::QuerySettings;
use crate::client_query::rate_limit_settings::RateLimitSettings;
use crate::subgraph_studio::APIKey;
use crate::subscriptions::Subscription;

use super::studio;
use super::subscriptions;
use super::AuthToken;

#[derive(Clone)]
pub struct AuthContext {
    /// Whether if the gateway should require payment for queries.
    ///
    /// This is used to disable the payment requirement on testnets. If this is `true`, then all queries require
    /// payment, unless they are subsidized or special.
    pub payment_required: bool,

    // Studio API keys
    pub api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    pub special_api_keys: Arc<HashSet<String>>,

    // Subscriptions
    pub subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
    pub special_query_key_signers: Arc<HashSet<Address>>,
    pub subscription_rate_per_query: u128,
    pub subscription_domains: Arc<HashMap<u64, Address>>,
}

impl FromRef<AuthContext> for studio::AuthContext {
    fn from_ref(auth: &AuthContext) -> Self {
        Self {
            studio_keys: auth.api_keys.clone(),
            special_api_keys: auth.special_api_keys.clone(),
        }
    }
}

impl FromRef<AuthContext> for subscriptions::AuthContext {
    fn from_ref(auth: &AuthContext) -> Self {
        Self {
            subscriptions: auth.subscriptions.clone(),
            special_signers: auth.special_query_key_signers.clone(),
            rate_per_query: auth.subscription_rate_per_query,
            subscription_domains: auth.subscription_domains.clone(),
        }
    }
}

impl AuthContext {
    pub fn create(
        payment_required: bool,
        api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
        special_api_keys: HashSet<String>,
        subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
        special_query_key_signers: HashSet<Address>,
        subscription_rate_per_query: u128,
        subscription_domains: HashMap<u64, Address>,
    ) -> Self {
        Self {
            payment_required,
            api_keys,
            special_api_keys: Arc::new(special_api_keys),
            special_query_key_signers: Arc::new(special_query_key_signers),
            subscriptions,
            subscription_rate_per_query,
            subscription_domains: Arc::new(subscription_domains),
        }
    }

    pub fn parse_auth_token(
        &self,
        input: &str,
    ) -> anyhow::Result<(AuthToken, Option<QuerySettings>, Option<RateLimitSettings>)> {
        // Ensure the bearer token is not empty
        if input.is_empty() {
            return Err(anyhow::anyhow!("Not found"));
        }

        // First, parse the bearer token as it was a Studio API key
        let ctx = studio::AuthContext::from_ref(self);
        if let Ok((auth, query_settings, rate_limit_settings)) =
            studio::parse_auth_token(&ctx, input)
        {
            return Ok((AuthToken::from(auth), query_settings, rate_limit_settings));
        }

        // Otherwise, parse the bearer token as a Subscriptions auth token
        let ctx = subscriptions::AuthContext::from_ref(self);
        if let Ok((auth, query_settings, rate_limit_settings)) =
            subscriptions::parse_auth_token(&ctx, input)
        {
            return Ok((AuthToken::from(auth), query_settings, rate_limit_settings));
        }

        Err(anyhow::anyhow!("Invalid auth token"))
    }

    pub fn check_auth_requirements(&self, token: &AuthToken) -> anyhow::Result<()> {
        match token {
            AuthToken::StudioApiKey(auth) => {
                let ctx = studio::AuthContext::from_ref(self);
                studio::check_auth_requirements(&ctx, auth)
            }
            AuthToken::SubscriptionsAuthToken(auth) => {
                let ctx = subscriptions::AuthContext::from_ref(self);
                subscriptions::check_auth_requirements(&ctx, auth)
            }
        }
    }
}
