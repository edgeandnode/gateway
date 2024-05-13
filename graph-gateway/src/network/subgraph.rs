//! The Graph network subgraph indexes the Graph network smart contract which is responsible,
//! among other things, to act as an on-chain registry for subgraphs and their deployments.
//!
//! This module contains the logic necessary to query the Graph to get the latest state of the
//! network subgraph.

use thegraph_core::client as subgraph_client;

/// The Graph network subgraph types.
///
/// <div class="warning">
/// These types are used to deserialize the response from the Graph network subgraph.
/// These types are not meant to be used directly by the gateway logic.
///
/// Please, DO NOT mix or merge them.
/// </div>
///
/// See: https://github.com/graphprotocol/graph-network-subgraph/blob/master/schema.graphql
pub mod types {
    pub mod fetch_subgraphs {
        use alloy_primitives::{Address, BlockNumber};
        use serde::Deserialize;
        use serde_with::serde_as;
        use thegraph_core::types::{DeploymentId, SubgraphId};

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
            pub version: u32,
            pub subgraph_deployment: SubgraphDeployment,
        }

        #[serde_as]
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Manifest {
            pub network: Option<String>,
            #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
            pub start_block: Option<BlockNumber>,
        }

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct SubgraphDeployment {
            #[serde(rename = "ipfsHash")]
            pub id: DeploymentId,
            #[serde(rename = "indexerAllocations")]
            pub allocations: Vec<Allocation>,
            pub manifest: Option<Manifest>,
            #[serde(default)]
            pub transferred_to_l2: bool,
        }

        #[serde_as]
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Allocation {
            pub id: Address,
            pub indexer: Indexer,
        }

        #[serde_as]
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Indexer {
            pub id: Address,
            pub url: Option<String>,
        }
    }

    pub mod fetch_indexers {
        use alloy_primitives::Address;
        use serde::Deserialize;
        use serde_with::serde_as;
        use thegraph_core::types::DeploymentId;

        #[serde_as]
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Indexer {
            pub id: Address,
            pub url: Option<String>,
            #[serde_as(as = "serde_with::DisplayFromStr")]
            pub staked_tokens: u128,
            pub allocations: Vec<Allocation>,
        }

        #[serde_as]
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct Allocation {
            pub id: Address,
            #[serde_as(as = "serde_with::DisplayFromStr")]
            pub allocated_tokens: u128,
            pub subgraph_deployment: SubgraphDeployment,
        }

        #[serde_as]
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct SubgraphDeployment {
            #[serde(rename = "ipfsHash")]
            pub id: DeploymentId,
        }
    }
}

/// The Graph network subgraph client.
pub struct Client {
    client: subgraph_client::Client,
    l2_transfer_support: bool,
}

impl Client {
    /// Creates a new [`Client`] instance.
    pub fn new(client: subgraph_client::Client, l2_transfer_support: bool) -> Self {
        Self {
            client,
            l2_transfer_support,
        }
    }

    /// Fetch the list of subgraphs (and deployments) from the network subgraph.
    ///
    /// Performs a paginated query to fetch the latest state of the network subgraph:
    /// > Get all subgraphs ordered by `id` in ascending order, with all its `versions` ordered by
    /// > `version` in ascending order, and all its `indexerAllocations` ordered by
    /// > `allocatedTokens` in descending order; laying the largest allocation first.
    #[allow(clippy::obfuscated_if_else)]
    pub async fn fetch_subgraphs(
        &mut self,
    ) -> anyhow::Result<Vec<types::fetch_subgraphs::Subgraph>> {
        // The following query's response must fulfill the following assumptions:
        // - Perform a paginated query based on the subgraph's ID. All subgraphs are ordered by ID.
        //   The next query must start from the last subgraph ID fetched in the previous query.
        // - All the subgraph's versions are ordered by `version` in descending order. We assume
        //   that the newest deployment version is the one with the highest version number.
        // - All the subgraph's `indexerAllocations` are ordered by `allocatedTokens` in descending
        //   order; the largest allocation must be the first one.
        //
        // ref#: 9936786a-e286-45f3-9190-8409d8389e88
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
                versions(orderBy: version, orderDirection: desc) {{
                    version
                    subgraphDeployment {{
                        ipfsHash
                        manifest {{
                            network
                            startBlock
                        }}
                        indexerAllocations(
                            first: 100
                            orderBy: allocatedTokens, orderDirection: desc
                            where: {{ status: Active }}
                        ) {{
                            id
                            indexer {{
                                id
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

        self.client
            .paginated_query(query, 200)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }

    /// Fetch the list of indexers from the network subgraph.
    ///
    /// Performs a paginated query to fetch the latest state of the network subgraph:
    ///
    /// > Get all indexers with at least one active allocation, with all its active allocations
    /// > ordered by `allocatedTokens` in descending order; laying the largest allocation first.
    pub async fn fetch_indexers(&mut self) -> anyhow::Result<Vec<types::fetch_indexers::Indexer>> {
        // The following query response must fulfill the following assumptions:
        // - Perform a paginated query based on the indexer's ID. All indexers are ordered by ID.
        //   The next query must start from the last indexer ID fetched in the previous query.
        // - Get all indexers with at least one active allocation.
        // - To get the largest allocation, the query must fetch all active allocations ordered by
        //  `allocatedTokens` in descending order; the largest allocation must be the first one.
        //
        // ref#: d260724b-a445-4842-964e-fb95062c119d
        let query = r#"
            indexers(
                block: $block
                orderBy: id, orderDirection: asc
                first: $first
                where: { 
                    id_gt: $last
                    allocationCount_gt: 0
                }
            ) {
                id
                url
                stakedTokens
                allocations(
                    first: 100,
                    orderBy: allocatedTokens, orderDirection: desc
                    where: { status: Active }
                ) {
                    id
                    allocatedTokens
                    subgraphDeployment {
                        ipfsHash
                    }
                }
            }
        "#;

        self.client
            .paginated_query(query, 200)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }
}
