use crate::subgraph_client::SubgraphClient;
use crate::subgraph_deployments::{self, SubgraphDeployments};
use eventuals::{self, EventualExt as _};
use indexer_selection::{IndexerInfo, Indexing};
use prelude::*;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Data {
    pub slashing_percentage: Eventual<PPM>,
    pub subgraph_deployments: SubgraphDeployments,
    pub deployment_indexers: Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    pub indexers: Eventual<Ptr<HashMap<Address, Arc<IndexerInfo>>>>,
    pub allocations: Eventual<Ptr<HashMap<Address, AllocationInfo>>>,
}

pub struct AllocationInfo {
    pub indexing: Indexing,
    pub allocated_tokens: GRT,
}

pub struct Client {
    subgraph_client: SubgraphClient,
    slashing_percentage: EventualWriter<PPM>,
    subgraph_deployments: EventualWriter<Ptr<subgraph_deployments::Inputs>>,
    deployment_indexers: EventualWriter<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    indexers: EventualWriter<Ptr<HashMap<Address, Arc<IndexerInfo>>>>,
    allocations: EventualWriter<Ptr<HashMap<Address, AllocationInfo>>>,
}

impl Client {
    pub fn create(subgraph_client: SubgraphClient) -> Data {
        let (slashing_percentage_tx, slashing_percentage_rx) = Eventual::new();
        let (subgraph_deployments_tx, subgraph_deployments_rx) = Eventual::new();
        let (deployment_indexers_tx, deployment_indexers_rx) = Eventual::new();
        let (indexers_tx, indexers_rx) = Eventual::new();
        let (allocations_tx, allocations_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
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
            subgraph_deployments: SubgraphDeployments {
                inputs: subgraph_deployments_rx,
            },
            deployment_indexers: deployment_indexers_rx,
            indexers: indexers_rx,
            allocations: allocations_rx,
        }
    }

    async fn poll_network_params(&mut self) -> Result<(), String> {
        let response = match self
            .subgraph_client
            .query::<GraphNetworkResponse>(
                &json!({ "query": "{ graphNetwork(id: \"1\") { slashingPercentage } }" }),
            )
            .await?
            .graph_network
        {
            Some(res) => res,
            None => return Err("Discarding empty update (graphNetwork)".to_string()),
        };
        let slashing_percentage = response.slashing_percentage.try_into()?;
        self.slashing_percentage.write(slashing_percentage);
        Ok(())
    }

    async fn poll_allocations(&mut self) -> Result<(), String> {
        let response = self
            .subgraph_client
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
                    }
                }
                "#,
            )
            .await?;
        let mut deployment_indexers = HashMap::<SubgraphDeploymentID, Vec<Address>>::new();
        let mut indexers = HashMap::<Address, Arc<IndexerInfo>>::new();
        let mut allocations = HashMap::<Address, AllocationInfo>::new();
        for allocation in &response {
            let url = match allocation
                .indexer
                .url
                .as_ref()
                .and_then(|url| url.parse::<URL>().ok())
            {
                Some(url) => url,
                None => continue,
            };

            match deployment_indexers.entry(allocation.subgraph_deployment.id) {
                Entry::Occupied(mut entry) => entry.get_mut().push(allocation.indexer.id),
                Entry::Vacant(entry) => {
                    entry.insert(vec![allocation.indexer.id]);
                }
            };
            indexers.insert(
                allocation.indexer.id,
                Arc::new(IndexerInfo {
                    url,
                    stake: allocation.indexer.staked_tokens.shift(),
                }),
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
        if deployment_indexers.is_empty() || indexers.is_empty() || allocations.is_empty() {
            return Err(format!(
                "Discarding empty update (deployment_indexers={}, indexers={}, allocations={})",
                deployment_indexers.len(),
                indexers.len(),
                allocations.len()
            ));
        }
        self.deployment_indexers
            .write(Ptr::new(deployment_indexers));
        self.indexers.write(Ptr::new(indexers));
        self.allocations.write(Ptr::new(allocations));
        Ok(())
    }

    async fn poll_subgraphs(&mut self) -> Result<(), String> {
        let response = self
            .subgraph_client
            .paginated_query::<SubgraphDeployment>(
                r#"
                subgraphDeployments(
                    block: $block
                    skip: $skip
                    first: $first
                    where: {activeSubgraphCount_gt: 0}
                ) {
                    ipfsHash
                    versions(
                      orderBy: version
                      orderDirection: asc
                      where: {subgraph_: {active: true, entityVersion: 2}}
                    ) {
                        subgraph {
                            currentVersion {
                                subgraphDeployment {
                                    ipfsHash
                                }
                            }
                        }
                    }
                }
                "#,
            )
            .await?;
        let current_deployments = parse_current_deployments(&response);
        if current_deployments.is_empty() {
            return Err("Discarding empty update (subgraph_deployments)".to_string());
        }
        let deployment_to_subgraphs = parse_deployment_subgraphs(response);

        self.subgraph_deployments
            .write(Ptr::new(subgraph_deployments::Inputs {
                current_deployments,
                deployment_to_subgraphs,
            }));
        Ok(())
    }
}

// The current deployment is a map of a SubgraphID to its _current_ SubgraphDeploymentID Qm hash.
// Iterate through the subgraphDeployments -> versions -> subgraph -> currentVersion -> subgraphDeployment
// grab map of SubgraphIDs to their current version SubgraphDeploymentID.
fn parse_current_deployments(
    subgraph_deployment_response: &[SubgraphDeployment],
) -> HashMap<SubgraphID, SubgraphDeploymentID> {
    subgraph_deployment_response
        .iter()
        .flat_map(|deployment| {
            deployment.versions.iter().map(|version| {
                (
                    version.subgraph.id,
                    version.subgraph.current_version.subgraph_deployment.id,
                )
            })
        })
        .collect()
}

fn parse_deployment_subgraphs(
    subgraph_deployment_response: Vec<SubgraphDeployment>,
) -> HashMap<SubgraphDeploymentID, Vec<SubgraphID>> {
    subgraph_deployment_response
        .into_iter()
        .map(|deployment| {
            let subgraphs = deployment
                .versions
                .into_iter()
                .map(|version| version.subgraph.id)
                .collect();
            (deployment.id, subgraphs)
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphNetworkResponse {
    graph_network: Option<GraphNetwork>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphNetwork {
    slashing_percentage: u32,
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
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Subgraph {
    id: SubgraphID,
    current_version: SubgraphCurrentVersion,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubgraphVersion {
    subgraph: Subgraph,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubgraphCurrentVersion {
    subgraph_deployment: SubgraphDeploymentIdOnly,
}

#[derive(Deserialize)]
struct SubgraphDeployment {
    #[serde(rename = "ipfsHash")]
    id: SubgraphDeploymentID,
    versions: Vec<SubgraphVersion>,
}

#[derive(Deserialize)]
struct SubgraphDeploymentIdOnly {
    #[serde(rename = "ipfsHash")]
    id: SubgraphDeploymentID,
}
