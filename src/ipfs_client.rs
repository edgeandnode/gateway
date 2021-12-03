use reqwest;
use std::{error::Error, sync::Arc};
use tokio::sync::Semaphore;
use url::Url;

pub struct IPFSClient {
    client: reqwest::Client,
    endpoint: Url,
    semaphore: Semaphore,
}

impl IPFSClient {
    pub fn new(client: reqwest::Client, endpoint: Url, max_concurrent: usize) -> Arc<Self> {
        Arc::new(Self {
            client,
            endpoint,
            semaphore: Semaphore::new(max_concurrent),
        })
    }

    pub async fn cat(&self, ipfs_hash: &str) -> Result<String, Box<dyn Error>> {
        let _permit = self.semaphore.acquire().await;
        let url = self.endpoint.join("api/v0/cat")?;
        self.client
            .post(format!("{}?arg={}", url, ipfs_hash))
            .send()
            .await
            .and_then(|response| response.error_for_status())?
            .text()
            .await
            .map_err(Into::into)
    }
}
