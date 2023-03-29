use crate::{
    manifest_client::SubgraphInfo,
    price_automation::QueryBudgetFactors,
    studio_client::{is_domain_authorized, APIKey, IndexerPreferences, QueryStatus},
    subscriptions::Subscription,
};
use graph_subscriptions::{eip712::DomainSeparator, TicketPayload};
use prelude::{
    anyhow::{anyhow, bail, ensure, Result},
    eventuals::EventualExt as _,
    tokio::sync::RwLock,
    *,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

pub struct AuthHandler {
    pub query_budget_factors: QueryBudgetFactors,
    pub api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    pub special_api_keys: HashSet<String>,
    pub api_key_payment_required: bool,
    pub subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
    pub subscriptions_domain_separator: Option<DomainSeparator>,
    pub subscription_query_counters: RwLock<HashMap<Address, AtomicUsize>>,
}

#[derive(Debug)]
pub struct QuerySettings {
    pub budget: USD,
    pub indexer_preferences: IndexerPreferences,
}

pub enum AuthToken {
    /// API key from the Subgraph Studio Database.
    ApiKey(Arc<APIKey>),
    /// Ticket associated with a subscription.
    Ticket(TicketPayload, Subscription),
}

impl AuthToken {
    pub fn api_key(&self) -> String {
        match self {
            Self::ApiKey(api_key) => api_key.key.clone(),
            Self::Ticket(payload, _) => payload
                .name
                .clone()
                .unwrap_or_else(|| payload.id.to_string()),
        }
    }
}

impl AuthHandler {
    pub fn create(
        query_budget_factors: QueryBudgetFactors,
        api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
        special_api_keys: HashSet<String>,
        api_key_payment_required: bool,
        subscriptions: Eventual<Ptr<HashMap<Address, Subscription>>>,
        subscriptions_domain_separator: Option<DomainSeparator>,
    ) -> &'static Self {
        let handler: &'static Self = Box::leak(Box::new(Self {
            query_budget_factors,
            api_keys,
            special_api_keys,
            api_key_payment_required,
            subscriptions,
            subscriptions_domain_separator,
            subscription_query_counters: RwLock::default(),
        }));

        // Reset counters every 10 seconds.
        // 5720d5ea-cfc3-4862-865b-52b4508a4c14
        eventuals::timer(Duration::from_secs(10))
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

    pub fn parse_token(&self, input: &str) -> Result<AuthToken> {
        ensure!(!input.is_empty(), "Not found");

        // We assume that Studio API keys are 32 hex digits.
        let mut api_key_buf = [0_u8; 16];
        if let Ok(()) = faster_hex::hex_decode(input.as_bytes(), &mut api_key_buf) {
            return self
                .api_keys
                .value_immediate()
                .unwrap_or_default()
                .get(input)
                .map(|api_key| AuthToken::ApiKey(api_key.clone()))
                .ok_or_else(|| anyhow!("Not found"));
        }

        let domain_separator = self
            .subscriptions_domain_separator
            .ok_or_else(|| anyhow!("Subscriptions not supported"))?;
        let (payload, _) = TicketPayload::from_ticket_base64(input.as_bytes(), &domain_separator)?;

        let user = Address(payload.user.unwrap_or(payload.signer).0);
        let subscription = self
            .subscriptions
            .value_immediate()
            .unwrap_or_default()
            .get(&user)
            .cloned()
            .ok_or_else(|| anyhow!("Subscription not found for user {}", user))?;

        let signer = Address(payload.signer.0);
        ensure!(
            (signer == user) || subscription.signers.contains(&signer),
            "Signer {} not authorized for user {}",
            signer,
            user,
        );

        Ok(AuthToken::Ticket(payload, subscription))
    }

    pub async fn check_token(
        &self,
        token: &AuthToken,
        subgraph_info: &SubgraphInfo,
        domain: &str,
    ) -> Result<()> {
        // Enforce the API key payment status, unless it's being subsidized.
        if let AuthToken::ApiKey(api_key) = &token {
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
            AuthToken::ApiKey(api_key) => api_key.deployments.clone(),
            AuthToken::Ticket(payload, _) => payload
                .allowed_deployments
                .iter()
                .flat_map(|s| s.split(','))
                .filter_map(|s| s.parse::<DeploymentId>().ok())
                .collect(),
        };
        tracing::debug!(?allowed_deployments);
        let allow_deployment = allowed_deployments.is_empty()
            || allowed_deployments.contains(&subgraph_info.deployment);
        ensure!(allow_deployment, "Deployment not authorized by API key");

        // Check subgraph allowlist
        let allowed_subgraphs: Vec<SubgraphId> = match token {
            AuthToken::ApiKey(api_key) => api_key.subgraphs.clone(),
            AuthToken::Ticket(payload, _) => payload
                .allowed_subgraphs
                .iter()
                .flat_map(|s| s.split(','))
                .filter_map(|s| s.parse::<SubgraphId>().ok())
                .collect(),
        };
        tracing::debug!(?allowed_subgraphs);
        // multiple subgraph ids can be resolved to the deployment Qm hash,
        // check if any of the SubgraphInfo.ids are in the api_key allowed subgraphs
        let allow_subgraph = allowed_subgraphs.is_empty() || {
            allowed_subgraphs
                .iter()
                .any(|subgraph_id| subgraph_info.ids.contains(subgraph_id))
        };
        ensure!(allow_subgraph, "Subgraph not authorized by API key");

        // Check domain allowlist
        let allowed_domains: Vec<&str> = match token {
            AuthToken::ApiKey(api_key) => api_key.domains.iter().map(|s| s.as_str()).collect(),
            AuthToken::Ticket(payload, _) => payload
                .allowed_domains
                .iter()
                .flat_map(|s| s.split(','))
                .collect(),
        };
        tracing::debug!(%domain, ?allowed_domains);
        let allow_domain =
            allowed_domains.is_empty() || is_domain_authorized(&allowed_domains, domain);
        ensure!(allow_domain, "Domain not authorized by API key");

        // Check rate limit for subscriptions. This step should be last to avoid invalid queries
        // taking up the rate limit.
        let (ticket_payload, subscription) = match token {
            AuthToken::ApiKey(_) => return Ok(()),
            AuthToken::Ticket(payload, subscription) => (payload, subscription),
        };
        let user = Address(ticket_payload.user.unwrap_or(ticket_payload.signer).0);
        let counters = match self.subscription_query_counters.try_read() {
            Ok(counters) => counters,
            // Just skip if we can't acquire the read lock. This is a relaxed operation anyway.
            Err(_) => return Ok(()),
        };
        match counters.get(&user) {
            Some(counter) => {
                let count = counter.fetch_add(1, atomic::Ordering::Relaxed);
                // Note that counters are for 10s intervals
                // 5720d5ea-cfc3-4862-865b-52b4508a4c14
                let limit = subscription.query_rate_limit as usize * 10;
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

    pub async fn query_settings(&self, token: &AuthToken, query_count: u64) -> QuerySettings {
        // This has to run even if we don't use the budget because it updates the query volume
        // estimate. This is important in the case that the user switches back to automated volume
        // discounting. Otherwise it will look like there is a long period of inactivity which would
        // increase the price.
        let budget = {
            match token {
                AuthToken::ApiKey(api_key) => api_key.usage.lock().await,
                AuthToken::Ticket(_, sub) => sub.usage.lock().await,
            }
            .budget_for_queries(query_count, &self.query_budget_factors)
        };

        let mut budget = USD::try_from(budget).unwrap();
        if let AuthToken::ApiKey(api_key) = token {
            if let Some(max_budget) = api_key.max_budget {
                // Security: Consumers can and will set their budget to unreasonably high values.
                // This `.min` prevents the budget from being set far beyond what it would be
                // automatically. The reason this is important is because sometimes queries are
                // subsidized and we would be at-risk to allow arbitrarily high values.
                budget = max_budget.min(budget * USD::try_from(10_u64).unwrap());
            }
        }

        let indexer_preferences = match token {
            AuthToken::ApiKey(api_key) => api_key.indexer_preferences.clone(),
            AuthToken::Ticket(_, _) => IndexerPreferences::default(),
        };

        QuerySettings {
            budget,
            indexer_preferences,
        }
    }
}
