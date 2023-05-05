use crate::subgraph_client;
use eventuals::{self, EventualExt as _};
use prelude::*;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Data {
    pub slashing_percentage: Eventual<PPM>,
    pub subgraphs: Eventual<Ptr<Vec<Subgraph>>>,
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
    pub url: String,
    pub staked_tokens: GRTWei,
}

pub struct Client {
    subgraph_client: subgraph_client::Client,
    slashing_percentage: EventualWriter<PPM>,
    subgraphs: EventualWriter<Ptr<Vec<Subgraph>>>,
    l2_migration_delay: Option<chrono::Duration>,
}

impl Client {
    pub fn create(
        subgraph_client: subgraph_client::Client,
        l2_migration_delay: Option<chrono::Duration>,
    ) -> Data {
        let (slashing_percentage_tx, slashing_percentage_rx) = Eventual::new();
        let (subgraphs_tx, subgraphs_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            slashing_percentage: slashing_percentage_tx,
            subgraphs: subgraphs_tx,
            l2_migration_delay,
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
                    if let Err(poll_subgraphs_err) = client.poll_subgraphs().await {
                        tracing::error!(%poll_subgraphs_err);
                    }
                }
            })
            .forever();
        Data {
            slashing_percentage: slashing_percentage_rx,
            subgraphs: subgraphs_rx,
        }
    }

    async fn poll_network_params(&mut self) -> Result<(), String> {
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

        let response = self
            .subgraph_client
            .query::<GraphNetworkResponse>(
                &json!({ "query": "{ graphNetwork(id: \"1\") { slashingPercentage } }" }),
            )
            .await?
            .graph_network
            .ok_or_else(|| "Discarding empty update (graphNetwork)".to_string())?;
        let slashing_percentage = response.slashing_percentage.try_into()?;
        self.slashing_percentage.write(slashing_percentage);
        Ok(())
    }

    async fn poll_subgraphs(&mut self) -> Result<(), String> {
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
                        indexerAllocations(where: { status: Active }) {
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
