use crate::price_automation::{VolumeEstimations, VolumeEstimator};
use eventuals::{self, EventualExt as _};
use prelude::*;
use serde::Deserialize;
use std::{collections::HashMap, error::Error, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Default)]
pub struct APIKey {
    pub id: i64,
    pub key: String,
    pub is_subsidized: bool,
    pub user_id: i64,
    pub user_address: Address,
    pub query_status: QueryStatus,
    pub max_budget: Option<USD>,
    pub deployments: Vec<DeploymentId>,
    pub subgraphs: Vec<SubgraphId>,
    pub domains: Vec<String>,
    pub indexer_preferences: IndexerPreferences,
    pub usage: Arc<Mutex<VolumeEstimator>>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryStatus {
    #[default]
    Inactive,
    Active,
    ServiceShutoff,
}

#[derive(Clone, Debug, Default)]
pub struct IndexerPreferences {
    pub freshness_requirements: f64,
    pub performance: f64,
    pub data_freshness: f64,
    pub economic_security: f64,
    pub price_efficiency: f64,
}

pub fn is_domain_authorized<S: AsRef<str>>(authorized: &[S], origin: &str) -> bool {
    authorized.iter().any(|authorized| {
        let pattern = authorized.as_ref().split('.');
        let origin = origin.split('.');
        let count = pattern.clone().count();
        if (count < 1) || (origin.clone().count() != count) {
            return false;
        }
        pattern.zip(origin).all(|(p, o)| (p == o) || (p == "*"))
    })
}

pub struct Actor {
    client: reqwest::Client,
    url: Url,
    auth: String,
    api_key_usage: VolumeEstimations<i64>,
    api_keys_writer: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
}

pub struct Data {
    pub api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
}

impl Actor {
    pub fn create(client: reqwest::Client, mut url: Url, auth: String) -> Data {
        let (api_keys_writer, api_keys) = Eventual::new();
        if !url.path().ends_with('/') {
            url.0.set_path(&format!("{}/", url.path()));
        }
        let actor = Arc::new(Mutex::new(Self {
            client,
            url,
            auth,
            api_key_usage: VolumeEstimations::new(),
            api_keys_writer,
        }));
        eventuals::timer(Duration::from_secs(30))
            .pipe_async(move |_| {
                let actor = actor.clone();
                async move {
                    let mut actor = actor.lock().await;
                    match actor.fetch_api_keys().await {
                        Ok(api_keys) => actor.api_keys_writer.write(Ptr::new(api_keys)),
                        Err(api_key_fetch_error) => tracing::error!(%api_key_fetch_error),
                    };
                }
            })
            .forever();
        Data { api_keys }
    }

    async fn fetch_api_keys(&mut self) -> Result<HashMap<String, Arc<APIKey>>, Box<dyn Error>> {
        let response = self
            .client
            .get(self.url.join("gateway-api-keys")?)
            .bearer_auth(&self.auth)
            .send()
            .await?
            .json::<GetGatewayApiKeysResponsePayload>()
            .await?;
        let api_keys = response
            .api_keys
            .into_iter()
            .filter_map(|api_key| {
                let mut indexer_preferences = IndexerPreferences::default();
                for preference in api_key.indexer_preferences {
                    match preference.name.as_str() {
                        "Fastest speed" => indexer_preferences.performance = preference.weight,
                        "Lowest price" => indexer_preferences.price_efficiency = preference.weight,
                        "Data freshness" => indexer_preferences.data_freshness = preference.weight,
                        "Economic security" => {
                            indexer_preferences.economic_security = preference.weight
                        }
                        unexpected_indexer_preference_name => {
                            tracing::warn!(%unexpected_indexer_preference_name)
                        }
                    }
                }
                let api_key = APIKey {
                    usage: self.api_key_usage.get(&api_key.id),
                    id: api_key.id,
                    key: api_key.key,
                    is_subsidized: api_key.is_subsidized,
                    user_id: api_key.user_id,
                    user_address: api_key.user_address.parse().ok()?,
                    query_status: api_key.query_status,
                    max_budget: api_key.max_budget.and_then(|b| USD::try_from(b).ok()),
                    subgraphs: api_key
                        .subgraphs
                        .into_iter()
                        .filter_map(|s| s.network_subgraph_id.parse::<SubgraphId>().ok())
                        .collect(),
                    deployments: api_key
                        .deployments
                        .into_iter()
                        .filter_map(|id| DeploymentId::from_ipfs_hash(&id))
                        .collect(),
                    domains: api_key
                        .domains
                        .into_iter()
                        .map(|domain| domain.domain)
                        .collect(),
                    indexer_preferences,
                };
                Some((api_key.key.clone(), Arc::new(api_key)))
            })
            .collect::<HashMap<String, Arc<APIKey>>>();

        tracing::info!(api_keys = api_keys.len());
        Ok(api_keys)
    }
}

#[derive(Deserialize)]
struct GetGatewayApiKeysResponsePayload {
    api_keys: Vec<GatewayApiKey>,
}

#[derive(Deserialize)]
struct GatewayApiKey {
    id: i64,
    key: String,
    is_subsidized: bool,
    user_id: i64,
    user_address: String,
    query_status: QueryStatus,
    max_budget: Option<f64>,
    #[serde(default)]
    indexer_preferences: Vec<GatewayIndexerPreference>,
    #[serde(default)]
    subgraphs: Vec<GatewaySubgraph>,
    #[serde(default)]
    deployments: Vec<String>,
    #[serde(default)]
    domains: Vec<GatewayDomain>,
}

#[derive(Deserialize)]
struct GatewayIndexerPreference {
    name: String,
    weight: f64,
}

#[derive(Deserialize)]
struct GatewaySubgraph {
    network_subgraph_id: String,
}

#[derive(Deserialize)]
struct GatewayDomain {
    domain: String,
}

#[cfg(test)]
mod test {
    use super::is_domain_authorized;

    #[test]
    fn authorized_domains() {
        let authorized_domains = ["example.com", "localhost", "a.b.c", "*.d.e"];
        let tests = [
            ("", false),
            ("example.com", true),
            ("subdomain.example.com", false),
            ("localhost", true),
            ("badhost", false),
            ("a.b.c", true),
            ("c", false),
            ("b.c", false),
            ("d.b.c", false),
            ("a", false),
            ("a.b", false),
            ("e", false),
            ("d.e", false),
            ("z.d.e", true),
        ];
        for (input, expected) in tests {
            assert_eq!(expected, is_domain_authorized(&authorized_domains, input));
        }
    }
}
