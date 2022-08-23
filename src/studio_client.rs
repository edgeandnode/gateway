use crate::{
    indexer_selection::IndexerPreferences,
    prelude::*,
    query_engine::{APIKey, QueryStatus, VolumeEstimator},
};
use eventuals::{self, EventualExt as _};
use reqwest;
use serde::Deserialize;
use std::{collections::HashMap, error::Error, sync::Arc};
use tokio::sync::Mutex;
use url::Url;

pub struct Actor {
    client: reqwest::Client,
    url: Url,
    auth: String,
    api_key_usage: VolumeEstimations,
    api_keys_writer: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
}

impl Actor {
    pub fn create(
        client: reqwest::Client,
        mut url: Url,
        auth: String,
    ) -> Eventual<Ptr<HashMap<String, Arc<APIKey>>>> {
        let (api_keys_writer, api_keys) = Eventual::new();
        if !url.path().ends_with("/") {
            url.set_path(&format!("{}/", url.path()));
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
        api_keys
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
        let api_keys = response.api_keys.into_iter().filter_map(|api_key| {
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
            Some(APIKey {
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
                    .filter_map(|s| Some((s.network_subgraph_id.parse::<SubgraphID>().ok()?, s.id)))
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
            })
        });
        Ok(api_keys.map(|v| (v.key.clone(), Arc::new(v))).collect())
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
