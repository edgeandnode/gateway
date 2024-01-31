use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};
use std::time::Duration;

use alloy_primitives::Address;
use axum::extract::FromRef;
use eventuals::{Eventual, EventualExt, Ptr};
use tokio::sync::RwLock;

use crate::subgraph_studio::APIKey;
use crate::subscriptions::Subscription;
use crate::topology::Deployment;

use super::studio;
use super::subscriptions;
use super::AuthToken;

#[derive(Clone)]
pub struct AuthContext {
    pub api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    pub special_api_keys: Arc<HashSet<String>>,
    pub special_query_key_signers: Arc<HashSet<Address>>,
    pub api_key_payment_required: bool,
    pub subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
    pub subscription_rate_per_query: u128,
    pub subscription_domains: Arc<HashMap<u64, Address>>,
    pub subscription_query_counters: Arc<RwLock<HashMap<Address, AtomicUsize>>>,
}

impl FromRef<AuthContext> for studio::AuthContext {
    fn from_ref(auth: &AuthContext) -> Self {
        Self {
            studio_keys: auth.api_keys.clone(),
            special_api_keys: auth.special_api_keys.clone(),
            api_key_payment_required: auth.api_key_payment_required,
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
            query_counters: auth.subscription_query_counters.clone(),
        }
    }
}

impl AuthContext {
    pub fn create(
        api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
        special_api_keys: HashSet<String>,
        special_query_key_signers: HashSet<Address>,
        api_key_payment_required: bool,
        subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
        subscription_rate_per_query: u128,
        subscription_domains: HashMap<u64, Address>,
    ) -> &'static Self {
        let handler: &'static Self = Box::leak(Box::new(Self {
            api_keys,
            special_api_keys: Arc::new(special_api_keys),
            special_query_key_signers: Arc::new(special_query_key_signers),
            api_key_payment_required,
            subscriptions,
            subscription_rate_per_query,
            subscription_domains: Arc::new(subscription_domains),
            subscription_query_counters: Default::default(),
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
        let auth_handler = studio::AuthContext::from_ref(self);
        if let Ok(api_key) = studio::parse_bearer_token(&auth_handler, input) {
            return Ok(AuthToken::StudioApiKey(api_key));
        }

        // Otherwise, parse the bearer token as a Subscriptions auth token
        let auth_handler = subscriptions::AuthContext::from_ref(self);
        if let Ok(claims) = subscriptions::parse_bearer_token(&auth_handler, input) {
            return Ok(AuthToken::SubscriptionsAuthToken(claims));
        }

        Err(anyhow::anyhow!("Invalid auth token"))
    }

    pub async fn check_token(
        &self,
        token: &AuthToken,
        deployments: &[Arc<Deployment>],
    ) -> anyhow::Result<()> {
        match token {
            AuthToken::StudioApiKey(api_key) => {
                let auth_handler = studio::AuthContext::from_ref(self);
                studio::check_token(&auth_handler, api_key, deployments).await
            }
            AuthToken::SubscriptionsAuthToken(auth_token) => {
                let auth_handler = subscriptions::AuthContext::from_ref(self);
                subscriptions::check_token(&auth_handler, auth_token, deployments).await
            }
        }
    }
}
