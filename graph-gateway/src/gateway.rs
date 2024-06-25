use core::fmt;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{bytes::Buf, Address};
use alloy_sol_types::Eip712Domain;
use anyhow::anyhow;
use axum::{
    async_trait,
    body::Bytes,
    http::{HeaderMap, StatusCode, Uri},
    response::Response,
};
use cost_model::{Context as AgoraContext, CostModel};
use eventuals::{Eventual, Ptr};
use gateway_common::utils::http_ext::HttpBuilderExt;
use gateway_framework::{
    auth::api_keys::APIKey,
    blocks::BlockConstraint,
    chain::Chain,
    config::ApiKeys,
    errors::{Error, IndexerError, UnavailableReason},
    gateway::http::{
        requests::{
            blocks::BlockRequirements,
            handler::{GatewayRequestContext, IncomingRequest},
        },
        DeterministicRequest, GatewayImpl, IndexerResponse, IndexingStatus,
    },
    graphql::error_response,
    http::attestation_header::GraphAttestation,
    indexing::Indexing,
    network::network_subgraph,
    scalar::ScalarReceipt,
    topology::network::{Deployment, Indexer},
};
use headers::ContentType;
use num_traits::ToPrimitive;
use serde::Deserialize;
use serde_json::value::RawValue;
use thegraph_core::{
    client as subgraph_client,
    types::{DeploymentId, SubgraphId},
};
use tokio::sync::watch;
use url::Url;

use crate::{
    block_constraints::{block_constraints, rewrite_query},
    config::Config,
    indexers::{indexing, query::query_indexer},
    indexings_blocklist::{self, indexings_blocklist},
    l2_forwarding::forward_request_to_l2,
    subgraph_studio,
};

pub struct SubgraphGatewayOptions {
    pub config: Config,
}

pub struct SubgraphGateway {
    pub config: Config,
    pub http_client: reqwest::Client,
}

impl SubgraphGateway {
    pub fn new(options: SubgraphGatewayOptions) -> Self {
        SubgraphGateway {
            config: options.config,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Query {
    pub query: String,
    pub variables: Option<Box<RawValue>>,
}

#[derive(Clone, Debug)]
pub struct SubgraphIndexingStatus {
    pub block: u64,
    pub min_block: Option<u64>,
    pub legacy_scalar: bool,
    pub cost_model: Option<Ptr<CostModel>>,
}

impl IndexingStatus for SubgraphIndexingStatus {
    fn block(&self) -> u64 {
        self.block
    }

    fn min_block(&self) -> Option<u64> {
        self.min_block
    }

    fn legacy_scalar(&self) -> bool {
        self.legacy_scalar
    }
}

pub struct SubgraphRequest {
    pub agora_context: AgoraContext<'static>,
    pub query: Query,
}

impl Debug for SubgraphRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubgraphRequest")
            .field("query", &self.query)
            .finish()
    }
}

impl Drop for SubgraphRequest {
    fn drop(&mut self) {}
}

#[async_trait]
impl GatewayImpl for SubgraphGateway {
    type IndexingStatus = SubgraphIndexingStatus;
    type Request = SubgraphRequest;

    async fn datasets(
        &self,
        network_subgraph: subgraph_client::Client,
        l2_transfer_support: bool,
    ) -> Eventual<Ptr<Vec<network_subgraph::Subgraph>>> {
        network_subgraph::Client::create(network_subgraph, l2_transfer_support).await
    }

    async fn indexing_statuses(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    ) -> Eventual<Ptr<HashMap<Indexing, Self::IndexingStatus>>> {
        indexing::statuses(
            deployments.clone(),
            self.http_client.clone(),
            self.config.min_graph_node_version.clone(),
            self.config.min_indexer_version.clone(),
        )
        .await
    }

    async fn indexings_blocklist(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
        indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
    ) -> Eventual<Ptr<HashSet<Indexing>>> {
        // Indexer blocklist
        //
        // Periodically check the defective POIs list against the network indexers
        // and update the indexers blocklist accordingly.
        match &self.config.poi_blocklist {
            Some(blocklist) if !blocklist.pois.is_empty() => {
                let pois = blocklist.pois.clone();
                let update_interval = blocklist
                    .update_interval
                    .map_or(indexings_blocklist::DEFAULT_UPDATE_INTERVAL, |min| {
                        Duration::from_secs(min * 60)
                    });

                indexings_blocklist(
                    self.http_client.clone(),
                    deployments.clone(),
                    indexers.clone(),
                    pois,
                    update_interval,
                )
                .await
            }
            _ => Eventual::from_value(Ptr::default()),
        }
    }

    async fn api_keys(&self) -> watch::Receiver<HashMap<String, APIKey>> {
        match self.config.common.api_keys {
            Some(ApiKeys::Endpoint {
                ref url, ref auth, ..
            }) => {
                subgraph_studio::api_keys(self.http_client.clone(), url.clone(), auth.0.clone())
                    .await
            }
            Some(ApiKeys::Fixed(ref api_keys)) => {
                watch::channel(
                    api_keys
                        .iter()
                        .map(|k| (k.key.clone(), k.clone()))
                        .collect(),
                )
                .1
            }
            None => watch::channel(Default::default()).1,
        }
    }

    async fn forward_request_to_l2(
        &self,
        l2_url: &Url,
        original_path: &Uri,
        headers: HeaderMap,
        body: Bytes,
        l2_subgraph_id: Option<SubgraphId>,
    ) -> Response<String> {
        forward_request_to_l2(
            &self.http_client,
            l2_url,
            original_path,
            headers,
            body,
            l2_subgraph_id,
        )
        .await
    }

    async fn parse_request(
        &self,
        _context: &GatewayRequestContext,
        request: &IncomingRequest,
    ) -> Result<Self::Request, Error> {
        let query: Query = serde_json::from_reader(request.body.as_ref().reader())
            .map_err(|err| Error::BadQuery(anyhow!("failed to parse request body: {err}")))?;

        let variables = query
            .variables
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        // This is safe as long as `SubgraphRequest` implements `Drop` and
        // drops the `agora_context` before the `query`
        let agora_context = AgoraContext::new(
            unsafe { std::mem::transmute::<&str, &'static str>(&query.query) },
            unsafe { std::mem::transmute::<&str, &'static str>(&variables) },
        )
        .map_err(|err| Error::BadQuery(anyhow!("{err}")))?;

        Ok(SubgraphRequest {
            agora_context,
            query,
        })
    }

    fn deterministic_request(
        &self,
        _context: &GatewayRequestContext,
        request: &Self::Request,
        chain: &Chain,
        block_requirements: &BlockRequirements,
        blocks_behind: u64,
    ) -> DeterministicRequest {
        DeterministicRequest {
            body: rewrite_query(
                chain,
                &request.agora_context,
                block_requirements,
                blocks_behind,
            ),
            headers: HeaderMap::default(),
        }
    }

    async fn block_constraints(
        &self,
        _context: &GatewayRequestContext,
        request: &Self::Request,
    ) -> Result<BTreeSet<BlockConstraint>, Error> {
        block_constraints(&request.agora_context)
    }

    fn indexer_request_fee(
        &self,
        _context: &GatewayRequestContext,
        request: &Self::Request,
        status: &Self::IndexingStatus,
    ) -> Result<u128, IndexerError> {
        match status
            .cost_model
            .as_ref()
            .map(|model| model.cost_with_context(&request.agora_context))
        {
            None => Ok(0),
            Some(Ok(fee)) => fee
                .to_u128()
                .ok_or(IndexerError::Unavailable(UnavailableReason::NoFee)),
            Some(Err(_)) => Err(IndexerError::Unavailable(UnavailableReason::NoFee)),
        }
    }

    async fn send_request_to_indexer(
        &self,
        _context: &GatewayRequestContext,
        deployment: &DeploymentId,
        url: &Url,
        receipt: &ScalarReceipt,
        attestation_domain: &Eip712Domain,
        request: DeterministicRequest,
    ) -> Result<IndexerResponse, IndexerError> {
        query_indexer(
            &self.http_client,
            deployment,
            url,
            receipt,
            attestation_domain,
            request.body,
        )
        .await
    }

    async fn finalize_response(&self, result: Result<IndexerResponse, Error>) -> Response<String> {
        match result {
            Ok(response) => Response::builder()
                .status(StatusCode::OK)
                .header_typed(ContentType::json())
                .header_typed(GraphAttestation(response.attestation))
                .body(response.client_response)
                .unwrap(),
            Err(err) => error_response(err),
        }
    }
}
