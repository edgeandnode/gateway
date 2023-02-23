use prelude::*;
use std::sync::Arc;
use tokio::sync::Semaphore;

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

    pub async fn cat(&self, ipfs_hash: &str) -> Result<String, reqwest::Error> {
        let _permit = self.semaphore.acquire().await;
        self.client
            .post(format!("{}{}", self.endpoint, ipfs_hash))
            .send()
            .await
            .and_then(|response| response.error_for_status())?
            .text()
            .await
            .map_err(Into::into)
    }
}
