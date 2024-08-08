//! The Graph network subgraph indexes the Graph network smart contract which is responsible,
//! among other things, to act as an on-chain registry for subgraphs and their deployments.
//!
//! This module contains the logic necessary to query the Graph to get the latest state of the
//! network subgraph.

use alloy_primitives::{BlockHash, BlockNumber};
use anyhow::{anyhow, bail, ensure, Context};
use custom_debug::CustomDebug;
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use serde_json::json;
use serde_with::serde_as;
use thegraph_graphql_http::http::response::Error as GqlError;
use types::Subgraph;
use url::Url;

use crate::{
    blocks::Block,
    config::Hidden,
    indexer_client::{IndexerAuth, IndexerClient},
    time::unix_timestamp,
};

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
    use alloy_primitives::BlockNumber;
    use serde::Deserialize;
    use serde_with::serde_as;
    use thegraph_core::types::{AllocationId, DeploymentId, IndexerId, SubgraphId};

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Subgraph {
        pub id: SubgraphId,
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
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Allocation {
        pub id: AllocationId,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub allocated_tokens: u128,
        pub indexer: Indexer,
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Indexer {
        pub id: IndexerId,
        pub url: Option<String>,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub staked_tokens: u128,
    }
}

#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct TrustedIndexer {
    /// network subgraph endpoint
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    /// free query auth token
    pub auth: Hidden<String>,
}

#[derive(Clone, Debug)]
enum BlockHeight {
    Hash(BlockHash),
    NumberGte(BlockNumber),
}

impl Serialize for BlockHeight {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut obj = s.serialize_map(Some(1))?;
        match self {
            Self::Hash(hash) => obj.serialize_entry("hash", hash)?,
            Self::NumberGte(number) => obj.serialize_entry("number_gte", number)?,
        }
        obj.end()
    }
}

/// The Graph network subgraph client.
pub struct Client {
    pub client: IndexerClient,
    pub indexers: Vec<TrustedIndexer>,
    pub page_size: usize,
    pub latest_block: Option<Block>,
}

impl Client {
    /// Fetch the list of subgraphs (and deployments) from the network subgraph.
    pub async fn fetch(&mut self) -> anyhow::Result<Vec<types::Subgraph>> {
        for indexer in &self.indexers.clone() {
            match self.fetch_from_indexer(indexer).await {
                Ok(results) => return Ok(results),
                Err(network_subgraph_query_err) => {
                    tracing::error!(indexer = %indexer.url, %network_subgraph_query_err);
                }
            };
        }
        bail!("trusted indexers exhausted");
    }

    async fn fetch_from_indexer(
        &mut self,
        indexer: &TrustedIndexer,
    ) -> anyhow::Result<Vec<types::Subgraph>> {
        // ref: 9936786a-e286-45f3-9190-8409d8389e88
        let query = r#"
            query ($block: Block_height!, $first: Int!, $last: String!) {
                meta: _meta(block: $block) { block { number hash timestamp } }
                results: subgraphs(
                    block: $block
                    orderBy: id, orderDirection: asc
                    first: $first
                    where: {
                        id_gt: $last
                        entityVersion: 2
                        versionCount_gte: 1
                        active: true
                    }
                ) {
                    id
                    versions(orderBy: version, orderDirection: desc) {
                        version
                        subgraphDeployment {
                            ipfsHash
                            manifest {
                                network
                                startBlock
                            }
                            indexerAllocations(
                                first: 100
                                orderBy: allocatedTokens, orderDirection: desc
                                where: { status: Active }
                            ) {
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
            }"#;

        #[derive(Debug, Deserialize)]
        pub struct QueryResponse {
            data: Option<QueryData>,
            #[serde(default)]
            errors: Vec<GqlError>,
        }
        #[derive(Debug, Deserialize)]
        pub struct QueryData {
            meta: Meta,
            results: Vec<Subgraph>,
        }
        #[derive(Debug, Deserialize)]
        pub struct Meta {
            block: Block,
        }

        debug_assert!(self.page_size > 0);
        let mut query_block: Option<Block> = None;
        let mut last_id: Option<String> = None;
        let mut results: Vec<Subgraph> = Default::default();

        loop {
            let block_height = match &query_block {
                Some(block) => BlockHeight::Hash(block.hash),
                None => BlockHeight::NumberGte(
                    self.latest_block.as_ref().map(|b| b.number).unwrap_or(0),
                ),
            };
            let page_query = json!({
                "query": query,
                "variables": {
                    "block": block_height,
                    "first": self.page_size,
                    "last": last_id.unwrap_or_default(),
                },
            });
            let response = self
                .client
                .query_indexer(
                    indexer.url.clone(),
                    IndexerAuth::Free(&indexer.auth),
                    &page_query.to_string(),
                )
                .await?;
            let response: QueryResponse =
                serde_json::from_str(&response.client_response).context("parse body")?;
            if !response.errors.is_empty() {
                bail!("{:?}", response.errors);
            }
            let mut data = response
                .data
                .ok_or_else(|| anyhow!("response missing data"))?;
            if let Some(block) = &query_block {
                ensure!(block == &data.meta.block);
            } else {
                ensure!(
                    data.meta.block.number
                        >= self.latest_block.as_ref().map(|b| b.number).unwrap_or(0),
                    "response block before latest",
                );
                ensure!(
                    (unix_timestamp() / 1_000).saturating_sub(data.meta.block.timestamp) < 120,
                    "response too far behind",
                );
                query_block = Some(data.meta.block);
            }
            last_id = data.results.last().map(|entry| entry.id.to_string());
            let page_len = data.results.len();
            results.append(&mut data.results);
            if page_len < self.page_size {
                break;
            }
        }

        self.latest_block = Some(query_block.unwrap());

        Ok(results)
    }
}
