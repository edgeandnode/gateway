use std::collections::HashMap;

use serde::Deserialize;
use tokio::{
    sync::watch,
    time::{Duration, MissedTickBehavior, interval},
};
use url::Url;

use crate::auth::{ApiKey, QueryStatus};

pub async fn api_keys(
    client: reqwest::Client,
    url: Url,
    auth: String,
) -> watch::Receiver<HashMap<String, ApiKey>> {
    let (tx, mut rx) = watch::channel(Default::default());
    let mut client = Client { client, url, auth };
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(30));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
    async fn fetch_api_keys(&mut self) -> anyhow::Result<HashMap<String, ApiKey>> {
        #[derive(Deserialize, Debug)]
        struct _ApiKeys {
            api_keys: Vec<_ApiKey>,
        }
        #[derive(Deserialize, Debug)]
        struct _ApiKey {
            key: String,
            user_address: String,
            query_status: QueryStatus,
            #[serde(default)]
            subgraphs: Vec<String>,
            #[serde(default)]
            domains: Vec<String>,
        }

        let response = self
            .client
            .get(self.url.clone())
            .bearer_auth(&self.auth)
            .send()
            .await?
            .json::<_ApiKeys>()
            .await?;
        let api_keys = response
            .api_keys
            .into_iter()
            .map(|api_key| {
                let api_key = ApiKey {
                    key: api_key.key,
                    user_address: api_key.user_address,
                    query_status: api_key.query_status,
                    domains: api_key.domains,
                    subgraphs: api_key
                        .subgraphs
                        .into_iter()
                        .filter_map(|s| s.parse().ok())
                        .collect(),
                };
                (api_key.key.clone(), api_key)
            })
            .collect::<HashMap<String, ApiKey>>();

        tracing::info!(api_keys = api_keys.len());
        Ok(api_keys)
    }
}
