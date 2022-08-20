use crate::{
    graphql, prelude::*, query_engine::Indexing, subgraph_deployments::SubgraphDeployments,
};
use eventuals::{self, EventualExt as _};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;
use url::Url;

#[derive(Clone)]
pub struct Data {
    pub slashing_percentage: Eventual<PPM>,
    pub subgraph_deployments: SubgraphDeployments,
    pub deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    pub indexers: Eventual<Ptr<HashMap<Address, IndexerInfo>>>,
    pub allocations: Eventual<Ptr<HashMap<Address, AllocationInfo>>>,
}

#[derive(Clone)]
pub struct IndexerInfo {
    pub url: Url,
    pub staked_tokens: GRT,
    pub delegated_tokens: GRT,
}

pub struct AllocationInfo {
    pub indexing: Indexing,
    pub allocated_tokens: GRT,
}

pub struct Client {
    network_subgraph: Url,
    http_client: reqwest::Client,
    latest_block: u64,
    slashing_percentage: EventualWriter<PPM>,
    subgraph_deployments: EventualWriter<Ptr<Vec<(SubgraphID, Vec<SubgraphDeploymentID>)>>>,
    deployment_indexers: EventualWriter<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    indexers: EventualWriter<Ptr<HashMap<Address, IndexerInfo>>>,
    allocations: EventualWriter<Ptr<HashMap<Address, AllocationInfo>>>,
}

impl Client {
    pub fn create(http_client: reqwest::Client, network_subgraph: Url) -> Data {
        let (slashing_percentage_tx, slashing_percentage_rx) = Eventual::new();
        let (subgraph_deployments_tx, subgraph_deployments_rx) = Eventual::new();
        let (deployment_indexers_tx, deployment_indexers_rx) = Eventual::new();
        let (indexers_tx, indexers_rx) = Eventual::new();
        let (allocations_tx, allocations_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            network_subgraph,
            http_client,
            latest_block: 11439999,
            slashing_percentage: slashing_percentage_tx,
            subgraph_deployments: subgraph_deployments_tx,
            deployment_indexers: deployment_indexers_tx,
            indexers: indexers_tx,
            allocations: allocations_tx,
        }));
        // 4e072dfe-5cb3-4f86-80f6-b64afeb9dcb2
        eventuals::timer(Duration::from_secs(30))
            .pipe_async(move |_| {
                let client = client.clone();
                async move {
                    let mut client = client.lock().await;
                    if let Err(poll_network_params_err) = client.poll_network_params().await {
                        tracing::error!(%poll_network_params_err);
                    }
                    if let Err(poll_allocations_err) = client.poll_allocations().await {
                        tracing::error!(%poll_allocations_err);
                    }
                    if let Err(poll_subgraphs_err) = client.poll_subgraphs().await {
                        tracing::error!(%poll_subgraphs_err);
                    }
                }
            })
            .forever();
        Data {
            slashing_percentage: slashing_percentage_rx,
            subgraph_deployments: SubgraphDeployments::new(subgraph_deployments_rx),
            deployment_indexers: deployment_indexers_rx,
            indexers: indexers_rx,
            allocations: allocations_rx,
        }
    }

    async fn poll_network_params(&mut self) -> Result<(), String> {
        let response = graphql::query::<GraphNetworksResponse, _>(
            &self.http_client,
            self.network_subgraph.clone(),
            &json!({ "query": "{ graphNetworks { slashingPercentage } }" }),
        )
        .await?
        .data
        .and_then(|data| data.graph_networks.into_iter().next())
        .ok_or("empty response")?;
        let slashing_percentage = response.slashing_percentage.try_into()?;
        self.slashing_percentage.write(slashing_percentage);
        Ok(())
    }

    async fn poll_allocations(&mut self) -> Result<(), String> {
        let response = self
            .paginated_query::<Allocation>(
                r#"
                allocations(
                    block: $block, skip: $skip, first: $first
                    where: { status: Active }
                ) {
                    id
                    allocatedTokens
                    subgraphDeployment { ipfsHash }
                    indexer {
                        id
                        url
                        stakedTokens
                        delegatedTokens
                    }
                }
                "#,
            )
            .await?;
        let mut deployment_indexers = HashMap::<SubgraphDeploymentID, Vec<Address>>::new();
        let mut indexers = HashMap::<Address, IndexerInfo>::new();
        let mut allocations = HashMap::<Address, AllocationInfo>::new();
        for allocation in &response {
            let url = match allocation
                .indexer
                .url
                .as_ref()
                .and_then(|url| url.parse::<Url>().ok())
            {
                Some(url) => url,
                None => continue,
            };

            match deployment_indexers.entry(allocation.subgraph_deployment.id.clone()) {
                Entry::Occupied(mut entry) => entry.get_mut().push(allocation.indexer.id),
                Entry::Vacant(entry) => {
                    entry.insert(vec![allocation.indexer.id]);
                }
            };
            indexers.insert(
                allocation.indexer.id.clone(),
                IndexerInfo {
                    url,
                    staked_tokens: allocation.indexer.staked_tokens.shift(),
                    delegated_tokens: allocation.indexer.delegated_tokens.shift(),
                },
            );
            allocations.insert(
                allocation.id,
                AllocationInfo {
                    indexing: Indexing {
                        deployment: allocation.subgraph_deployment.id,
                        indexer: allocation.indexer.id,
                    },
                    allocated_tokens: allocation.allocated_tokens.shift(),
                },
            );
        }
        self.deployment_indexers
            .write(Ptr::new(deployment_indexers));
        self.indexers.write(Ptr::new(indexers));
        self.allocations.write(Ptr::new(allocations));
        Ok(())
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
                    versions(orderBy: version, orderDirection: asc) {
                        subgraphDeployment { ipfsHash }
                    }
                }
                "#,
            )
            .await?;
        let subgraph_deployments = response
            .into_iter()
            .map(|subgraph| {
                let versions = subgraph
                    .versions
                    .into_iter()
                    .map(|v| v.subgraph_deployment.id)
                    .collect();
                (subgraph.id, versions)
            })
            .collect();
        self.subgraph_deployments
            .write(Ptr::new(subgraph_deployments));
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
            let response = graphql::query::<PaginatedQueryResponse<T>, _>(
                &self.http_client,
                self.network_subgraph.clone(),
                &json!({
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
                }),
            )
            .await?;
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
#[serde(rename_all = "camelCase")]
struct GraphNetworksResponse {
    graph_networks: Vec<GraphNetwork>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphNetwork {
    slashing_percentage: u32,
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
struct Allocation {
    id: Address,
    allocated_tokens: GRTWei,
    subgraph_deployment: SubgraphDeployment,
    indexer: Indexer,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Indexer {
    id: Address,
    url: Option<String>,
    staked_tokens: GRTWei,
    delegated_tokens: GRTWei,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Subgraph {
    id: SubgraphID,
    versions: Vec<SubgraphVersion>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubgraphVersion {
    subgraph_deployment: SubgraphDeployment,
}

#[derive(Deserialize)]
struct SubgraphDeployment {
    #[serde(rename = "ipfsHash")]
    id: SubgraphDeploymentID,
}
