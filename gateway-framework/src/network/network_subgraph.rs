use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use anyhow::anyhow;
use eventuals::{self, Eventual, EventualExt as _, EventualWriter, Ptr};
use serde::Deserialize;
use thegraph::{
    client as subgraph_client,
    types::{DeploymentId, SubgraphId, UDecimal18},
};
use tokio::sync::Mutex;

pub struct Data {
    pub network_params: NetworkParams,
    pub subgraphs: Eventual<Ptr<Vec<Subgraph>>>,
}

pub struct NetworkParams {
    pub slashing_percentage: UDecimal18,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subgraph {
    pub id: SubgraphId,
    pub id_on_l2: Option<SubgraphId>,
    pub versions: Vec<SubgraphVersion>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubgraphVersion {
    pub subgraph_deployment: SubgraphDeployment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubgraphDeployment {
    #[serde(rename = "ipfsHash")]
    pub id: DeploymentId,
    #[serde(rename = "indexerAllocations")]
    pub allocations: Vec<Allocation>,
    #[serde(default)]
    pub transferred_to_l2: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub id: Address,
    pub allocated_tokens: U256,
    pub indexer: Indexer,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Indexer {
    pub id: Address,
    pub url: Option<String>,
    pub staked_tokens: U256,
}

pub struct Client {
    subgraph_client: subgraph_client::Client,
    subgraphs: EventualWriter<Ptr<Vec<Subgraph>>>,
    // TODO: remove when L2 subgraph transfer support is on mainnet network subgraphs
    l2_transfer_support: bool,
}

impl Client {
    pub async fn create(
        subgraph_client: subgraph_client::Client,
        l2_transfer_support: bool,
    ) -> anyhow::Result<Data> {
        let (subgraphs_tx, subgraphs_rx) = Eventual::new();
        let client = Arc::new(Mutex::new(Client {
            subgraph_client,
            subgraphs: subgraphs_tx,
            l2_transfer_support,
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
            .query::<GraphNetworkResponse>(query)
            .await
            .map_err(|err| anyhow!(err))?
            .graph_network
            .ok_or_else(|| anyhow!("Discarding empty update (graphNetwork)"))?;

        Ok(NetworkParams {
            slashing_percentage: UDecimal18::from_raw_u256(
                U256::from(response.slashing_percentage) * U256::from(1_000_000_000_000_u128),
            ),
        })
    }

    #[allow(clippy::obfuscated_if_else)]
    async fn poll_subgraphs(&mut self) -> Result<(), String> {
        // last allocation is latest by indexing: 9936786a-e286-45f3-9190-8409d8389e88
        let query = format!(
            r#"
            subgraphs(
                block: $block
                orderBy: id, orderDirection: asc
                first: $first
                where: {{
                    id_gt: $last
                    entityVersion: 2
                    {}
                }}
            ) {{
                id
                {}
                versions(orderBy: version, orderDirection: asc) {{
                    subgraphDeployment {{
                        ipfsHash
                        indexerAllocations(
                            first: 100
                            orderBy: createdAt, orderDirection: asc
                            where: {{ status: Active }}
                        ) {{
                            id
                            allocatedTokens
                            indexer {{
                                id
                                url
                                stakedTokens
                            }}
                        }}
                        {}
                    }}
                }}
            }}
        "#,
            self.l2_transfer_support
                .then_some("")
                .unwrap_or("active: true"),
            self.l2_transfer_support.then_some("idOnL2").unwrap_or(""),
            self.l2_transfer_support
                .then_some("transferredToL2")
                .unwrap_or(""),
        );

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
