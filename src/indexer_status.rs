use crate::{graphql, indexer_selection::Indexing, network_subgraph::IndexerInfo, prelude::*};
use eventuals::EventualExt as _;
use reqwest;
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::{spawn, sync::Mutex};
use url::Url;

pub struct Data {
    pub indexings: Eventual<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

pub struct IndexingStatus {
    block: BlockPointer,
    synced: bool,
    fatal_error: Option<String>,
}

pub struct Actor {
    client: reqwest::Client,
    indexings: EventualWriter<Ptr<HashMap<Indexing, IndexingStatus>>>,
}

impl Actor {
    pub fn create(
        client: reqwest::Client,
        indexers: Eventual<Ptr<HashMap<Address, IndexerInfo>>>,
    ) -> Data {
        let (indexings_tx, indexings_rx) = Eventual::new();
        let actor = Arc::new(Mutex::new(Actor {
            client,
            indexings: indexings_tx,
        }));
        spawn(async move {
            // Joining this eventual with a timer is unnecessary, so long as the Ptr value is
            // updated at regular intervals.
            indexers
                .pipe_async(move |indexers| {
                    let actor = actor.clone();
                    async move {
                        let actor = actor.lock().await;
                        let mut indexing_statuses = HashMap::<Indexing, IndexingStatus>::new();
                        // TODO: filter out incompatible & geoblocked indexers
                        for (indexer, info) in indexers.iter() {
                            let result = actor.query_status(&info.url).await;
                            let response = match result {
                                Ok(response) => response,
                                Err(_) => continue,
                            };
                            for (deployment, status) in response {
                                indexing_statuses.insert(
                                    Indexing {
                                        deployment,
                                        indexer: indexer.clone(),
                                    },
                                    status,
                                );
                            }
                        }
                    }
                })
                .forever();
        });
        Data {
            indexings: indexings_rx,
        }
    }

    async fn query_status(
        &self,
        url: &str,
    ) -> Result<Vec<(SubgraphDeploymentID, IndexingStatus)>, String> {
        let url = Url::from_str(url)
            .and_then(|url| url.join("status"))
            .map_err(|err| err.to_string())?;
        let query = r#"{
            indexingStatuses(subgraphs: []) {
                subgraph
                synced
                fatalError { message }
                chains { ... on EthereumIndexingStatus { latestBlock { number hash } } }
            }
        }"#;
        let response = graphql::query::<IndexerStatusResponse, _>(
            &self.client,
            url,
            &json!({ "query": query }),
        )
        .await?;
        let response = response.data.ok_or_else(|| {
            response
                .errors
                .unwrap_or_default()
                .into_iter()
                .map(|err| err.message)
                .collect::<Vec<String>>()
                .join(", ")
        })?;
        Ok(response
            .indexing_statuses
            .into_iter()
            .filter_map(|status| {
                let deployment = status.subgraph;
                let block = &status.chains.get(0)?.latest_block;
                let status = IndexingStatus {
                    block: BlockPointer {
                        number: block.number.parse().ok()?,
                        hash: block.hash.clone(),
                    },
                    synced: status.synced,
                    fatal_error: status.fatal_error,
                };
                Some((deployment, status))
            })
            .collect())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexerStatusResponse {
    indexing_statuses: Vec<IndexingStatusResponse>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexingStatusResponse {
    subgraph: SubgraphDeploymentID,
    synced: bool,
    fatal_error: Option<String>,
    chains: Vec<ChainStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChainStatus {
    latest_block: BlockStatus,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlockStatus {
    number: String,
    hash: Bytes32,
}
