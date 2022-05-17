use crate::prelude::*;
use eventuals::{self, EventualExt as _};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use url::Url;

pub struct Data {
    pub current_deployments: Eventual<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>>,
}

pub struct Client {
    network_subgraph: Url,
    http_client: reqwest::Client,
    latest_block: u64,
    current_deployments: EventualWriter<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>>,
}

impl Client {
    pub fn create(http_client: reqwest::Client, network_subgraph: Url) -> Data {
        let (current_deployments_tx, current_deployments_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            network_subgraph,
            http_client,
            latest_block: 0,
            current_deployments: current_deployments_tx,
        }));
        eventuals::timer(Duration::from_secs(5))
            .pipe_async(move |_| {
                let client = client.clone();
                async move {
                    let mut client = client.lock().await;
                    if let Err(poll_subgraphs_err) = client.poll_subgraphs().await {
                        tracing::error!(%poll_subgraphs_err);
                    }
                }
            })
            .forever();
        Data {
            current_deployments: current_deployments_rx,
        }
    }

    async fn poll_subgraphs(&mut self) -> Result<(), String> {
        let response = self
            .paginated_query::<Subgraph>(
                r#"
                subgraphs(
                    block: $block, skip: $skip, first: $first
                    where: { active: true }
                ) {
                    id
                    currentVersion { subgraphDeployment { ipfsHash } }
                }
                "#,
            )
            .await?;
        let current_deployments = response
            .into_iter()
            .map(|subgraph| (subgraph.id, subgraph.current_version.subgraph_deployment.id))
            .collect();
        self.current_deployments
            .write(Ptr::new(current_deployments));
        Ok(())
    }

    async fn paginated_query<T: for<'de> Deserialize<'de>>(
        &mut self,
        query: &'static str,
    ) -> Result<Vec<T>, String> {
        let batch_size: u32 = 1000;
        let mut index: u32 = 0;
        let mut query_block: Option<BlockPointer> = None;
        let mut results = Vec::new();
        loop {
            let block = query_block
                .as_ref()
                .map(|block| json!({ "hash": block.hash }))
                .unwrap_or(json!({ "number_gte": self.latest_block }));
            let response = self
                .http_client
                .post(self.network_subgraph.clone())
                .json(&json!({
                    "query": format!(r#"
                        query q($block: Block_height!, $skip: Int!, $first: Int!) {{
                            meta: _meta(block: $block) {{ block {{ number hash }} }}
                            results: {}
                        }}"#,
                        query,
                    ),
                    "variables": {
                        "block": block,
                        "skip": index * batch_size,
                        "first": batch_size,
                    },
                }))
                .send()
                .await
                .map_err(|err| err.to_string())?
                .json::<GraphQLResponse<PaginatedQueryResponse<T>>>()
                .await
                .map_err(|err| err.to_string())?;
            let errors = response
                .errors
                .unwrap_or_default()
                .into_iter()
                .map(|err| err.message)
                .collect::<Vec<String>>();
            if errors
                .iter()
                .any(|err| err.contains("no block with that hash found"))
            {
                tracing::info!("Reorg detected. Restarting query to try a new block.");
                index = 0;
                query_block = None;
                continue;
            }
            if !errors.is_empty() {
                return Err(errors.join(", "));
            }
            let mut data = match response.data {
                Some(data) if !data.results.is_empty() => data,
                _ => break,
            };
            index += 1;
            query_block = Some(data.meta.block);
            results.append(&mut data.results);
        }
        if let Some(block) = query_block {
            self.latest_block = block.number;
        }
        Ok(results)
    }
}

#[derive(Deserialize)]
struct GraphQLResponse<T> {
    data: Option<T>,
    errors: Option<Vec<Error>>,
}

#[derive(Deserialize)]
struct Error {
    message: String,
}

#[derive(Deserialize)]
struct Meta {
    block: BlockPointer,
}

#[derive(Deserialize)]
struct PaginatedQueryResponse<T> {
    meta: Meta,
    results: Vec<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Subgraph {
    id: SubgraphID,
    current_version: CurrentVersion,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CurrentVersion {
    subgraph_deployment: SubgraphDeployment,
}

#[derive(Deserialize)]
struct SubgraphDeployment {
    #[serde(rename = "ipfsHash")]
    id: SubgraphDeploymentID,
}