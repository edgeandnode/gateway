use std::{collections::HashMap, error::Error, sync::Arc};

use alloy_primitives::Address;
use gateway_framework::auth::methods::api_keys::{APIKey, QueryStatus};
use ordered_float::NotNan;
use serde::Deserialize;
use tokio::{
    sync::watch,
    time::{interval, Duration},
};
use url::Url;

pub async fn api_keys(
    client: reqwest::Client,
    url: Url,
    auth: String,
) -> watch::Receiver<HashMap<String, Arc<APIKey>>> {
    let (tx, mut rx) = watch::channel(Default::default());
    let mut client = Client { client, url, auth };
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(30));
        loop {
            interval.tick().await;

            match client.fetch_api_keys().await {
                Ok(api_keys) => {
                    if let Err(api_keys_send_err) = tx.send(api_keys) {
                        tracing::error!(%api_keys_send_err);
                    }
                }
                Err(api_key_fetch_error) => tracing::error!(%api_key_fetch_error),
            };
        }
    });

    rx.wait_for(|api_keys| !api_keys.is_empty()).await.unwrap();
    rx
}

struct Client {
    client: reqwest::Client,
    url: Url,
    auth: String,
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
            .get(self.url.clone())
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
