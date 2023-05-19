use crate::subgraph_client;
use eventuals::{self, EventualExt as _};
use prelude::{anyhow::anyhow, *};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Data {
    pub network_params: NetworkParams,
    pub subgraphs: Eventual<Ptr<Vec<Subgraph>>>,
}

pub struct NetworkParams {
    pub slashing_percentage: PPM,
}

#[derive(Debug, Deserialize)]
pub struct Subgraph {
    pub id: SubgraphId,
    pub versions: Vec<SubgraphVersion>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubgraphVersion {
    pub subgraph_deployment: SubgraphDeployment,
}

#[derive(Debug, Deserialize)]
pub struct SubgraphDeployment {
    #[serde(rename = "ipfsHash")]
    pub id: DeploymentId,
    #[serde(rename = "indexerAllocations")]
    pub allocations: Vec<Allocation>,
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
    l2_migration_delay: Option<chrono::Duration>,
}

impl Client {
    pub async fn create(
        subgraph_client: subgraph_client::Client,
        l2_migration_delay: Option<chrono::Duration>,
    ) -> anyhow::Result<Data> {
        let (subgraphs_tx, subgraphs_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            subgraphs: subgraphs_tx,
            l2_migration_delay,
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
                versions(orderBy: version, orderDirection: asc) {
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
                    }
                }
            }
        "#;

        let subgraphs = self
            .subgraph_client
            .paginated_query::<Subgraph>(query)
            .await?;

        // TODO: apply migration delay when `startedTransferToL2` field is added.
        // let now = chrono::Utc::now();
        // let migrated_away = response
        //     .iter()
        //     .filter_map(|deployment| {
        //         let l2_migration_delay = self.l2_migration_delay?;
        //         // Subgraph deployments may be associated with multiple subgraphs. We only stop
        //         // service to the subgraph deployment if all it's associated subgraphs have
        //         // migrated. And we do so after the delay, starting from the latest
        //         // `startedMigrationToL2At` timestamp of the associated subgraphs.
        //         let latest_migration = deployment
        //             .versions
        //             .iter()
        //             .map(|v| v.subgraph.started_migration_to_l2_at)
        //             .collect::<Option<Vec<chrono::DateTime<Utc>>>>()?
        //             .into_iter()
        //             .max()?;
        //         if latest_migration > (now - l2_migration_delay) {
        //             return None;
        //         }
        //         Some(deployment.id)
        //     })
        //     .collect();

        if subgraphs.is_empty() {
            return Err("Discarding empty update (subgraph_deployments)".to_string());
        }

        self.subgraphs.write(Ptr::new(subgraphs));
        Ok(())
    }
}
