use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::Address;
use axum::extract::FromRef;
use eventuals::{Eventual, Ptr};

use super::{
    methods::{
        api_keys::{self, APIKey},
        subscriptions,
    },
    AuthToken, QuerySettings,
};
use crate::{http::middleware::RateLimitSettings, subscriptions::Subscription};

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

impl FromRef<AuthContext> for api_keys::AuthContext {
    fn from_ref(auth: &AuthContext) -> Self {
        Self {
            api_keys: auth.api_keys.clone(),
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

    /// Waits for the API keys to be fetched.
    ///
    /// Internally, it waits for the `api_keys` eventual value to resolve.
    pub async fn wait_for_api_keys(&self) -> anyhow::Result<()> {
        self.api_keys
            .value()
            .await
            .map_err(|_| anyhow::anyhow!("api keys fetch failed"))?;
        Ok(())
    }

    pub fn parse_auth_token(
        &self,
        input: &str,
    ) -> anyhow::Result<(AuthToken, Option<QuerySettings>, Option<RateLimitSettings>)> {
        // Ensure the bearer token is not empty
        if input.is_empty() {
            return Err(anyhow::anyhow!("not found"));
        }

        // First, parse the bearer token as if it is an API key
        let ctx = api_keys::AuthContext::from_ref(self);
        if let Ok((auth, query_settings, rate_limit_settings)) =
            api_keys::parse_auth_token(&ctx, input)
        {
            return Ok((AuthToken::from(auth), query_settings, rate_limit_settings));
        }

        // Otherwise, parse the bearer token as a Subscriptions auth token
        let ctx = subscriptions::AuthContext::from_ref(self);
        subscriptions::parse_auth_token(&ctx, input).map(
            |(auth, query_settings, rate_limit_settings)| {
                (AuthToken::from(auth), query_settings, rate_limit_settings)
            },
        )
    }

    pub fn check_auth_requirements(&self, token: &AuthToken) -> anyhow::Result<()> {
        match token {
            AuthToken::ApiKey(auth) => {
                let ctx = api_keys::AuthContext::from_ref(self);
                api_keys::check_auth_requirements(&ctx, auth)
            }
            AuthToken::SubscriptionsAuthToken(auth) => {
                let ctx = subscriptions::AuthContext::from_ref(self);
                subscriptions::check_auth_requirements(&ctx, auth)
            }
        }
    }
}
