//! The Graph network subgraph indexes the Graph network smart contract which is responsible,
//! among other things, to act as an on-chain registry for subgraphs and their deployments.
//!
//! This module contains the logic necessary to query the Graph to get the latest state of the
//! network subgraph.

use anyhow::{Context, anyhow, bail, ensure};
use custom_debug::CustomDebug;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use serde_json::json;
use serde_with::serde_as;
use thegraph_core::alloy::primitives::{BlockHash, BlockNumber, BlockTimestamp};
use thegraph_graphql_http::http::response::Error as GqlError;
use types::Subgraph;
use url::Url;

use crate::{
    blocks::Block,
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
    use serde::Deserialize;
    use serde_with::serde_as;
    use thegraph_core::{
        CollectionId, DeploymentId, IndexerId, SubgraphId, alloy::primitives::BlockNumber,
    };

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Subgraph {
        pub id: SubgraphId,
        // V2 Horizon fields (populated programmatically, not from JSON)
        #[serde(skip)]
        pub _service_provider: Option<ServiceProvider>,
        #[serde(skip)]
        pub _allocations: Vec<ServiceAllocation>,
        // V1 compatibility (for gradual migration)
        #[serde(default)]
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
        #[serde(default)]
        pub payments_escrows: Vec<PaymentsEscrow>,
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PaymentsEscrow {
        pub id: String,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub balance: u128,
        pub indexer: Indexer,
        pub collections: Vec<Collection>,
        pub _subgraph_deployment: SubgraphDeployment,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Collection {
        pub id: CollectionId,
        pub status: String,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct ServiceProvider {
        pub _id: IndexerId,
        pub _url: Option<String>,
        #[serde(rename = "stakedTokens")]
        pub _staked_tokens: String,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct ServiceAllocation {
        pub _id: String,
        pub _tokens: String,
        #[serde(rename = "subgraphDeployment")]
        pub _subgraph_deployment: DeploymentInfo,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct DeploymentInfo {
        #[serde(rename = "ipfsHash")]
        pub _ipfs_hash: DeploymentId,
        pub _manifest: Option<ManifestInfo>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct ManifestInfo {
        pub _network: Option<String>,
        #[serde(rename = "startBlock")]
        pub _start_block: String,
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
    /// Complete network subgraph endpoint URL (e.g., http://indexer:7601/subgraphs/id/Qmc2Cb...)
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    /// free query auth token
    pub auth: String,
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
    pub max_lag_seconds: u64,
}

impl Client {
    /// Fetch the list of subgraphs (and deployments) from the network subgraph.
    pub async fn fetch(&mut self) -> anyhow::Result<Vec<types::Subgraph>> {
        for indexer in &self.indexers.clone() {
            match self.fetch_from_indexer(indexer).await {
                Ok(results) => return Ok(results),
                Err(network_subgraph_query_err) => {
                    tracing::error!(
                        indexer = %indexer.url,
                        network_subgraph_query_err = format!("{network_subgraph_query_err:#}",
                    ));
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
                subgraphs(
                    block: $block
                    first: $first
                    orderBy: id, orderDirection: asc
                    where: { id_gt: $last }
                ) {
                    id
                    versions {
                        version
                        subgraphDeployment {
                            id
                            ipfsHash
                            manifest {
                                network
                                startBlock
                            }
                            indexerAllocations {
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
            subgraphs: Vec<SubgraphWithVersions>,
        }

        #[derive(Debug, Deserialize)]
        pub struct SubgraphWithVersions {
            pub id: String,
            pub versions: Vec<SubgraphVersionWithDeployment>,
        }

        #[derive(Debug, Deserialize)]
        pub struct SubgraphVersionWithDeployment {
            pub version: u32,
            #[serde(rename = "subgraphDeployment")]
            pub subgraph_deployment: DeploymentWithAllocations,
        }

        #[derive(Debug, Deserialize)]
        pub struct DeploymentWithAllocations {
            pub id: String,
            #[serde(rename = "ipfsHash")]
            pub ipfs_hash: String,
            pub manifest: Option<ManifestInfo>,
            #[serde(rename = "indexerAllocations")]
            pub indexer_allocations: Vec<AllocationWithIndexer>,
        }

        #[derive(Debug, Deserialize)]
        pub struct AllocationWithIndexer {
            pub id: String,
            #[serde(rename = "allocatedTokens")]
            pub allocated_tokens: String,
            pub indexer: IndexerInfo,
        }

        #[derive(Debug, Deserialize)]
        pub struct IndexerInfo {
            pub id: thegraph_core::IndexerId,
            pub url: Option<String>,
            #[serde(rename = "stakedTokens")]
            pub staked_tokens: String,
        }

        #[derive(Debug, Deserialize)]
        pub struct DeploymentInfo {
            #[serde(rename = "ipfsHash")]
            pub _ipfs_hash: thegraph_core::DeploymentId,
            pub _manifest: Option<ManifestInfo>,
        }

        #[derive(Debug, Deserialize)]
        pub struct ManifestInfo {
            pub network: String,
            #[serde(rename = "startBlock")]
            pub start_block: String,
        }
        #[derive(Debug, Deserialize)]
        pub struct Meta {
            block: PartialBlock,
        }
        #[derive(Debug, Deserialize)]
        pub struct PartialBlock {
            pub number: BlockNumber,
            pub hash: BlockHash,
            pub timestamp: Option<BlockTimestamp>,
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
            // Use trusted indexer URL directly - it already contains the complete network subgraph endpoint
            let network_subgraph_url = indexer.url.clone();

            // Debug logging for network subgraph URL usage
            tracing::debug!(
                network_subgraph_url = %network_subgraph_url,
                "using trusted indexer URL for network subgraph query"
            );

            let response = self
                .client
                .query_indexer(
                    network_subgraph_url,
                    IndexerAuth::Free(&indexer.auth),
                    &page_query.to_string(),
                )
                .await?;
            tracing::trace!(
                response.original_response,
                response.client_response,
                ?response.errors,
            );
            let response: QueryResponse =
                serde_json::from_str(&response.client_response).context("parse body")?;
            if !response.errors.is_empty() {
                bail!("{:?}", response.errors);
            }
            let data = response
                .data
                .ok_or_else(|| anyhow!("response missing data"))?;
            let block = Block {
                number: data.meta.block.number,
                hash: data.meta.block.hash,
                timestamp: data
                    .meta
                    .block
                    .timestamp
                    .ok_or_else(|| anyhow!("response missing block timestamp"))?,
            };
            if let Some(query_block) = &query_block {
                ensure!(query_block == &block);
            } else {
                ensure!(
                    data.meta.block.number
                        >= self.latest_block.as_ref().map(|b| b.number).unwrap_or(0),
                    "response block before latest",
                );
                ensure!(
                    (unix_timestamp() / 1_000).saturating_sub(block.timestamp)
                        < self.max_lag_seconds,
                    "response too far behind",
                );
                query_block = Some(block);
            }
            // Process V2 subgraphs data with correct SubgraphId → DeploymentId mapping
            let subgraphs = data.subgraphs;
            last_id = subgraphs.last().map(|entry| entry.id.clone());
            let page_len = subgraphs.len();

            results.extend(subgraphs.into_iter().map(|subgraph| {
                // Parse the SubgraphId from the response
                let subgraph_id = subgraph
                    .id
                    .parse::<thegraph_core::SubgraphId>()
                    .unwrap_or_else(|_| {
                        // Fallback: create SubgraphId from string bytes
                        use thegraph_core::{SubgraphId, alloy::primitives::FixedBytes};
                        let mut bytes = [0u8; 32];
                        let id_bytes = subgraph.id.as_bytes();
                        let copy_len = id_bytes.len().min(32);
                        bytes[..copy_len].copy_from_slice(&id_bytes[..copy_len]);
                        SubgraphId::new(FixedBytes::from(bytes))
                    });

                types::Subgraph {
                    id: subgraph_id,
                    _service_provider: None, // V2 doesn't use service providers
                    _allocations: Vec::new(), // V2 data structure doesn't populate this
                    versions: subgraph
                        .versions
                        .into_iter()
                        .map(|version| {
                            // Log deployment info for debugging
                            tracing::debug!(
                                deployment_id = %version.subgraph_deployment.id,
                                ipfs_hash = %version.subgraph_deployment.ipfs_hash,
                                "processing V2 subgraph deployment"
                            );

                            let deployment_id = version
                                .subgraph_deployment
                                .ipfs_hash
                                .parse::<thegraph_core::DeploymentId>()
                                .expect("V2 IPFS hash should be valid DeploymentId");

                            types::SubgraphVersion {
                                version: version.version,
                                subgraph_deployment: types::SubgraphDeployment {
                                    id: deployment_id,
                                    manifest: version.subgraph_deployment.manifest.map(|m| {
                                        types::Manifest {
                                            network: Some(m.network),
                                            start_block: m.start_block.parse().unwrap_or(0),
                                        }
                                    }),
                                    payments_escrows: version
                                        .subgraph_deployment
                                        .indexer_allocations
                                        .into_iter()
                                        .map(|alloc| {
                                            let allocation_id = alloc.id.clone();
                                            types::PaymentsEscrow {
                                                id: alloc.id,
                                                balance: alloc
                                                    .allocated_tokens
                                                    .parse()
                                                    .unwrap_or(0),
                                                indexer: types::Indexer {
                                                    id: alloc.indexer.id,
                                                    url: alloc.indexer.url,
                                                    staked_tokens: alloc
                                                        .indexer
                                                        .staked_tokens
                                                        .parse()
                                                        .unwrap_or(0),
                                                },
                                                collections: vec![types::Collection {
                                                    id: {
                                                        // Convert allocation ID to collection ID using standard left-padding
                                                        use thegraph_core::AllocationId;
                                                        let allocation_parsed = allocation_id
                                                            .parse::<AllocationId>()
                                                            .expect(
                                                                "V2 allocation ID should be valid",
                                                            );
                                                        allocation_parsed.into() // Uses standard left-padding conversion
                                                    },
                                                    status: "Active".to_string(), // Default status
                                                }],
                                                _subgraph_deployment: types::SubgraphDeployment {
                                                    id: deployment_id,
                                                    manifest: None, // Avoid circular reference
                                                    payments_escrows: Vec::new(), // Avoid circular reference
                                                },
                                            }
                                        })
                                        .collect(),
                                },
                            }
                        })
                        .collect(),
                }
            }));
            if page_len < self.page_size {
                break;
            }
        }

        self.latest_block = Some(query_block.unwrap());

        Ok(results)
    }
}
