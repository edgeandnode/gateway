use crate::price_automation::VolumeEstimator;
use eventuals::{self, EventualExt as _};
use prelude::*;
use reqwest;
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
    pub deployments: Vec<SubgraphDeploymentID>,
    pub subgraphs: Vec<(SubgraphID, i32)>,
    pub domains: Vec<(String, i32)>,
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

pub fn is_domain_authorized<'a>(
    authorized: impl IntoIterator<Item = &'a str>,
    origin: &str,
) -> bool {
    authorized.into_iter().any(|authorized| {
        let pattern = authorized.split('.');
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
    url: URL,
    auth: String,
    api_key_usage: VolumeEstimations,
    api_keys_writer: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
    usd_to_grt_writer: EventualWriter<USD>,
}

pub struct Data {
    pub api_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
    pub usd_to_grt: Eventual<USD>,
}

impl Actor {
    pub fn create(client: reqwest::Client, mut url: URL, auth: String) -> Data {
        let (api_keys_writer, api_keys) = Eventual::new();
        let (usd_to_grt_writer, usd_to_grt) = Eventual::new();
        if !url.path().ends_with("/") {
            url.0.set_path(&format!("{}/", url.path()));
        }
        let actor = Arc::new(Mutex::new(Self {
            client,
            url,
            auth,
            api_key_usage: VolumeEstimations::new(),
            api_keys_writer,
            usd_to_grt_writer,
        }));
        eventuals::timer(Duration::from_secs(30))
            .pipe_async(move |_| {
                let actor = actor.clone();
                async move {
                    let mut actor = actor.lock().await;
                    match actor.fetch_usd_to_grt().await {
                        Ok(usd_to_grt) => actor.usd_to_grt_writer.write(usd_to_grt),
                        Err(usd_to_grt_fetch_error) => tracing::error!(%usd_to_grt_fetch_error),
                    };
                    match actor.fetch_api_keys().await {
                        Ok(api_keys) => actor.api_keys_writer.write(Ptr::new(api_keys)),
                        Err(api_key_fetch_error) => tracing::error!(%api_key_fetch_error),
                    };
                }
            })
            .forever();
        Data {
            api_keys,
            usd_to_grt,
        }
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
                    usage: self.api_key_usage.get(&api_key.key),
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
                        .filter_map(|s| {
                            Some((s.network_subgraph_id.parse::<SubgraphID>().ok()?, s.id))
                        })
                        .collect(),
                    deployments: api_key
                        .deployments
                        .into_iter()
                        .filter_map(|id| SubgraphDeploymentID::from_ipfs_hash(&id))
                        .collect(),
                    domains: api_key
                        .domains
                        .into_iter()
                        .map(|domain| (domain.domain, domain.id))
                        .collect(),
                    indexer_preferences,
                };
                Some((api_key.key.clone(), Arc::new(api_key)))
            })
            .collect::<HashMap<String, Arc<APIKey>>>();

        tracing::info!(api_keys = api_keys.len());
        Ok(api_keys)
    }

    async fn fetch_usd_to_grt(&mut self) -> Result<USD, Box<dyn Error>> {
        #[derive(Deserialize)]
        struct GRTPrice {
            usd: f64,
        }
        let price = self
            .client
            .get(self.url.join("grt-price")?)
            .bearer_auth(&self.auth)
            .send()
            .await?
            .json::<GRTPrice>()
            .await?
            .usd;
        // Check that the float value isn't completely outside of reasonable bounds.
        if (price < 1e-3) || (price > 1e3) {
            return Err(format!("Conversion rate out of range ({})", price).into());
        }
        let usd_to_grt =
            USD::try_from(price.recip()).map_err(|_| "Failed to convert price to decimal value")?;
        tracing::debug!(%usd_to_grt, source_price = price);
        Ok(usd_to_grt)
    }
}

struct VolumeEstimations {
    by_api_key: HashMap<String, Arc<Mutex<VolumeEstimator>>>,
    decay_list: Arc<parking_lot::Mutex<Option<Vec<Arc<Mutex<VolumeEstimator>>>>>>,
}

impl Drop for VolumeEstimations {
    fn drop(&mut self) {
        *self.decay_list.lock() = None;
    }
}

impl VolumeEstimations {
    pub fn new() -> Self {
        let decay_list = Arc::new(parking_lot::Mutex::new(Some(Vec::new())));
        let result = Self {
            by_api_key: HashMap::new(),
            decay_list: decay_list.clone(),
        };

        // Every 2 minutes, call decay on every VolumeEstimator in our collection.
        // This task will finish when the VolumeEstimations is dropped, because
        // drop sets the decay_list to None which breaks the loop.
        tokio::spawn(async move {
            loop {
                let start = Instant::now();
                let len = if let Some(decay_list) = decay_list.lock().as_deref() {
                    decay_list.len()
                } else {
                    return;
                };
                for i in 0..len {
                    let item = if let Some(decay_list) = decay_list.lock().as_deref() {
                        decay_list[i].clone()
                    } else {
                        return;
                    };
                    item.lock().await.decay();
                }
                let next = start + Duration::from_secs(120);
                tokio::time::sleep_until(next).await;
            }
        });
        result
    }

    pub fn get(&mut self, key: &str) -> Arc<Mutex<VolumeEstimator>> {
        match self.by_api_key.get(key) {
            Some(exist) => exist.clone(),
            None => {
                let result = Arc::new(Mutex::new(VolumeEstimator::default()));
                self.by_api_key.insert(key.to_owned(), result.clone());
                self.decay_list
                    .lock()
                    .as_mut()
                    .unwrap()
                    .push(result.clone());
                result
            }
        }
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
    id: i32,
    network_subgraph_id: String,
}

#[derive(Deserialize)]
struct GatewayDomain {
    id: i32,
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
            assert_eq!(expected, is_domain_authorized(authorized_domains, input));
        }
    }
}
