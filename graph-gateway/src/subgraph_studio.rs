use std::{collections::HashMap, error::Error, sync::Arc};

use alloy_primitives::Address;
use eventuals::{self, Eventual, EventualExt as _, EventualWriter, Ptr};
use ordered_float::NotNan;
use serde::Deserialize;
use thegraph::types::{DeploymentId, SubgraphId};
use tokio::{sync::Mutex, time::Duration};
use toolshed::url::Url;

#[derive(Clone, Debug, Default)]
pub struct APIKey {
    pub key: String,
    pub user_address: Address,
    pub query_status: QueryStatus,
    pub max_budget_usd: Option<NotNan<f64>>,
    pub deployments: Vec<DeploymentId>,
    pub subgraphs: Vec<SubgraphId>,
    pub domains: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryStatus {
    #[default]
    Active,
    ServiceShutoff,
    FreePlanExceeded,
}

pub fn api_keys(
    client: reqwest::Client,
    mut url: Url,
    auth: String,
) -> Eventual<Ptr<HashMap<String, Arc<APIKey>>>> {
    let (writer, reader) = Eventual::new();
    if !url.path().ends_with('/') {
        url.0.set_path(&format!("{}/", url.path()));
    }
    let client: &'static Mutex<Client> = Box::leak(Box::new(Mutex::new(Client {
        client,
        url,
        auth,
        api_keys_writer: writer,
    })));
    eventuals::timer(Duration::from_secs(30))
        .pipe_async(move |_| async move {
            let mut client = client.lock().await;
            match client.fetch_api_keys().await {
                Ok(api_keys) => client.api_keys_writer.write(Ptr::new(api_keys)),
                Err(api_key_fetch_error) => tracing::error!(%api_key_fetch_error),
            };
        })
        .forever();
    reader
}

struct Client {
    client: reqwest::Client,
    url: Url,
    auth: String,
    api_keys_writer: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
}

impl Client {
    async fn fetch_api_keys(&mut self) -> Result<HashMap<String, Arc<APIKey>>, Box<dyn Error>> {
        #[derive(Deserialize)]
        struct GetGatewayApiKeysResponsePayload {
            api_keys: Vec<GatewayApiKey>,
        }
        #[derive(Deserialize)]
        struct GatewayApiKey {
            key: String,
            user_address: Address,
            query_status: QueryStatus,
            max_budget: Option<f64>,
            #[serde(default)]
            subgraphs: Vec<String>,
            #[serde(default)]
            deployments: Vec<String>,
            #[serde(default)]
            domains: Vec<String>,
        }

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
            .map(|api_key| {
                let api_key = APIKey {
                    key: api_key.key,
                    user_address: api_key.user_address,
                    query_status: api_key.query_status,
                    max_budget_usd: api_key.max_budget.and_then(|b| NotNan::new(b).ok()),
                    subgraphs: api_key
                        .subgraphs
                        .into_iter()
                        .filter_map(|s| s.parse().ok())
                        .collect(),
                    deployments: api_key
                        .deployments
                        .into_iter()
                        .filter_map(|s| s.parse().ok())
                        .collect(),
                    domains: api_key.domains,
                };
                (api_key.key.clone(), Arc::new(api_key))
            })
            .collect::<HashMap<String, Arc<APIKey>>>();

        tracing::info!(api_keys = api_keys.len());
        Ok(api_keys)
    }
}
