use crate::subgraph_client;
use chrono::{DateTime, Utc};
use eventuals::{self, EventualExt as _};
use prelude::{anyhow::anyhow, *};
use serde::Deserialize;
use serde_json::json;
use serde_with::serde_as;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Data {
    pub network_params: NetworkParams,
    pub subgraphs: Eventual<Ptr<Vec<Subgraph>>>,
}

pub struct NetworkParams {
    pub slashing_percentage: PPM,
}

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subgraph {
    pub id: SubgraphId,
    pub versions: Vec<SubgraphVersion>,
    pub id_on_l2: Option<SubgraphId>,
    #[serde_as(as = "Option<serde_with::TimestampSeconds<String>>")]
    pub started_transfer_to_l2_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubgraphVersion {
    pub metadata_hash: Option<String>,
    pub subgraph_deployment: SubgraphDeployment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubgraphDeployment {
    #[serde(rename = "ipfsHash")]
    pub id: DeploymentId,
    #[serde(rename = "indexerAllocations")]
    pub allocations: Vec<Allocation>,
    pub transferred_to_l2: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub id: Address,
    pub allocated_tokens: GRTWei,
    pub indexer: Indexer,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Indexer {
    pub id: Address,
    pub url: Option<String>,
    pub staked_tokens: GRTWei,
}

pub struct Client {
    subgraph_client: subgraph_client::Client,
    subgraphs: EventualWriter<Ptr<Vec<Subgraph>>>,
}

impl Client {
    pub async fn create(subgraph_client: subgraph_client::Client) -> anyhow::Result<Data> {
        let (subgraphs_tx, subgraphs_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            subgraphs: subgraphs_tx,
        }));

        let network_params = client.lock().await.network_params().await?;

        // 4e072dfe-5cb3-4f86-80f6-b64afeb9dcb2
        eventuals::timer(Duration::from_secs(30))
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

        Ok(Data {
            network_params,
            subgraphs: subgraphs_rx,
        })
    }

    async fn network_params(&mut self) -> anyhow::Result<NetworkParams> {
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
        let query = r#"{ graphNetwork(id: "1") { slashingPercentage } }"#;
        let response = self
            .subgraph_client
            .query::<GraphNetworkResponse>(&json!({ "query": query }))
            .await
            .map_err(|err| anyhow!(err))?
            .graph_network
            .ok_or_else(|| anyhow!("Discarding empty update (graphNetwork)"))?;

        Ok(NetworkParams {
            slashing_percentage: response
                .slashing_percentage
                .try_into()
                .map_err(|_| anyhow!("Failed to parse slashingPercentage"))?,
        })
    }

    async fn poll_subgraphs(&mut self) -> Result<(), String> {
        // TODO: `indexerAllocations(first: 500` is for the MIPs program. Under normal circumstances
        // we would not expect so many indexers per deployment.
        let query = r#"
            subgraphs(
                block: $block
                orderBy: id, orderDirection: asc
                first: $first
                where: {
                    id_gt: $last
                    active: true
                    entityVersion: 2
                }
            ) {
                id
                idOnL2
                startedTransferToL2At
                versions(orderBy: version, orderDirection: asc) {
                    metadataHash
                    subgraphDeployment {
                        ipfsHash
                        indexerAllocations(first: 500, where: { status: Active }) {
                            id
                            allocatedTokens
                            indexer {
                                id
                                url
                                stakedTokens
                            }
                        }
                        transferredToL2
                    }
                }
            }
        "#;

        let subgraphs = self
            .subgraph_client
            .paginated_query::<Subgraph>(query)
            .await?;

        if subgraphs.is_empty() {
            return Err("Discarding empty update (subgraph_deployments)".to_string());
        }

        self.subgraphs.write(Ptr::new(subgraphs));
        Ok(())
    }
}
