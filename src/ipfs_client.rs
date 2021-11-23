use reqwest;
use std::sync::Arc;
use url::Url;

pub struct IPFSClient {
    client: reqwest::Client,
    endpoint: Url,
}

impl IPFSClient {
    pub fn new(client: reqwest::Client, endpoint: Url) -> Arc<Self> {
        Arc::new(Self { client, endpoint })
    }

    pub async fn cat(&self, ipfs_hash: &str) -> Result<String, reqwest::Error> {
        self.client
            .get(format!("{}/api/v0/cat?arg={}", self.endpoint, ipfs_hash))
            .send()
            .await
            .and_then(|response| response.error_for_status())?
            .text()
            .await
    }
}
