//! The Graph network subgraph indexes the Graph network smart contract which is responsible,
//! among other things, to act as an on-chain registry for subgraphs and their deployments.
//!
//! This module contains the logic necessary to query the Graph to get the latest state of the
//! network subgraph.

use paginated_client::PaginatedClient;
use thegraph_graphql_http::graphql::IntoDocument;

pub mod core_paginated_client;
pub mod indexers_list_paginated_client;
pub mod paginated_client;

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
    use alloy_primitives::{Address, BlockNumber};
    use serde::Deserialize;
    use serde_with::serde_as;
    use thegraph_core::types::{DeploymentId, SubgraphId};

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Subgraph {
        pub id: SubgraphId,
        pub id_on_l2: Option<SubgraphId>,
        pub versions: Vec<SubgraphVersion>,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SubgraphVersion {
        pub version: u32,
        pub subgraph_deployment: SubgraphDeployment,
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Manifest {
        pub network: Option<String>,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub start_block: BlockNumber,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SubgraphDeployment {
        #[serde(rename = "ipfsHash")]
        pub id: DeploymentId,
        pub manifest: Option<Manifest>,
        #[serde(rename = "indexerAllocations")]
        pub allocations: Vec<Allocation>,
        #[serde(default)]
        pub transferred_to_l2: bool,
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Allocation {
        pub id: Address,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub allocated_tokens: u128,
        pub indexer: Indexer,
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Indexer {
        pub id: Address,
        pub url: Option<String>,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub staked_tokens: u128,
    }
}

/// The Graph network subgraph client.
pub struct Client<C> {
    client: C,
    l2_transfer_support: bool,
}

impl<C> Client<C>
where
    C: PaginatedClient + Send + Sync + 'static,
{
    /// Creates a new [`Client`] instance.
    pub fn new(client: C, l2_transfer_support: bool) -> Self {
        Self {
            client,
            l2_transfer_support,
        }
    }

    /// Fetch the list of subgraphs (and deployments) from the network subgraph.
    ///
    /// Performs a paginated query to fetch the latest state of the network subgraph:
    /// > Get all subgraphs ordered by `id` in ascending order, filtering out subgraphs with no
    /// > versions and with all its `versions` ordered by `version` in ascending order, and all
    /// > its `indexerAllocations` ordered by `allocatedTokens` in descending order; laying the
    /// > largest allocation first.
    #[allow(clippy::obfuscated_if_else)]
    pub async fn fetch(&self) -> anyhow::Result<Vec<types::Subgraph>> {
        // The following query's response must fulfill the following assumptions:
        // - Perform a paginated query based on the subgraph's ID. All subgraphs are ordered by ID.
        //   The next query must start from the last subgraph ID fetched in the previous query.
        // - Get all subgraphs with at least one version.
        // - All the subgraph's versions are ordered by `version` in descending order. We assume
        //   that the newest deployment version is the one with the highest version number.
        // - All the subgraph's `indexerAllocations` are ordered by `allocatedTokens` in descending
        //   order; the largest allocation must be the first one.
        //
        // ref#: 9936786a-e286-45f3-9190-8409d8389e88
        let query = indoc::formatdoc! {
            r#" subgraphs(
                block: $block
                orderBy: id, orderDirection: asc
                first: $first
                where: {{
                    id_gt: $last
                    entityVersion: 2
                    versionCount_gte: 1
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
            }}"#,
            self.l2_transfer_support
                .then_some("")
                .unwrap_or("active: true"),
            self.l2_transfer_support.then_some("idOnL2").unwrap_or(""),
            self.l2_transfer_support
                .then_some("transferredToL2")
                .unwrap_or(""),
        };

        self.client
            .paginated_query(query.into_document(), 1000)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod it_fetch_and_deserialize;
}
