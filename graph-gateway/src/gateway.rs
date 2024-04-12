use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{bytes::Buf, Address};
use anyhow::anyhow;
use axum::{
    async_trait,
    body::{Body, Bytes},
    http::{header, HeaderMap, StatusCode, Uri},
    response::Response,
};
use cost_model::{Context as AgoraContext, CostModel};
use eventuals::{Eventual, EventualExt as _, Ptr};
use gateway_common::{types::Indexing, utils::http_ext::HttpBuilderExt};
use gateway_framework::{
    auth::methods::api_keys::APIKey,
    blocks::BlockConstraint,
    chain::Chain,
    errors::{Error, IndexerError, UnavailableReason},
    gateway::http::{
        ApiKeys, BlockRequirements, GatewayImpl, GatewayRequest, GatewayRequestContext,
        IndexerResponse, IndexingStatus, SelectionInfo,
    },
    graphql::error_response,
    http::attestation_header::GraphAttestation,
    network::network_subgraph,
    reporting::CLIENT_REQUEST_TARGET,
    topology::network::{Deployment, Indexer},
};
use graphql::{
    graphql_parser::query::{OperationDefinition, Selection},
    IntoStaticValue, StaticValue,
};
use headers::ContentType;
use num_traits::ToPrimitive;
use serde::Deserialize;
use serde_json::value::RawValue;
use thegraph_core::{
    client as subgraph_client,
    types::{DeploymentId, SubgraphId},
};
use url::Url;

use crate::{
    block_constraints::field_constraint,
    config::Config,
    indexer_client::IndexerClient,
    indexers::indexing,
    indexings_blocklist::{self, indexings_blocklist},
    subgraph_studio,
};

pub struct SubgraphGatewayOptions {
    pub config: Config,
}

pub struct SubgraphGateway {
    pub config: Config,
    pub indexer_client: IndexerClient,
    pub http_client: reqwest::Client,
}

impl SubgraphGateway {
    pub fn new(options: SubgraphGatewayOptions) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        SubgraphGateway {
            config: options.config,
            indexer_client: IndexerClient {
                client: http_client.clone(),
            },
            http_client,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Query {
    pub query: String,
    pub variables: Option<Box<RawValue>>,
}

pub struct SubgraphRequest {
    pub agora_context: AgoraContext<'static, String>,
}

pub struct DeterministicSubgraphRequest {}

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
}

#[async_trait]
impl GatewayImpl for SubgraphGateway {
    type IndexingStatus = SubgraphIndexingStatus;
    type Request = SubgraphRequest;
    type DeterministicRequest = DeterministicSubgraphRequest;

    async fn publications(
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

    async fn legacy_indexers(
        &self,
        indexing_statuses: Eventual<Ptr<HashMap<Indexing, Self::IndexingStatus>>>,
    ) -> Eventual<Ptr<HashSet<Address>>> {
        indexing_statuses.clone().map(|statuses| async move {
            let legacy_indexers: HashSet<Address> = statuses
                .iter()
                .filter(|(_, status)| status.legacy_scalar)
                .map(|(indexing, _)| indexing.indexer)
                .collect();
            Ptr::new(legacy_indexers)
        })
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

    async fn api_keys(&self) -> Eventual<Ptr<HashMap<String, Arc<APIKey>>>> {
        match self.config.common.api_keys.clone() {
            Some(ApiKeys::Endpoint { url, auth, .. }) => {
                subgraph_studio::api_keys(self.http_client.clone(), url, auth.0)
            }
            Some(ApiKeys::Fixed(api_keys)) => Eventual::from_value(Ptr::new(
                api_keys
                    .into_iter()
                    .map(|k| (k.key.clone(), k.into()))
                    .collect(),
            )),
            None => Eventual::from_value(Ptr::default()),
        }
    }

    async fn forward_request_to_l2(
        &self,
        l2_url: &Url,
        original_path: &Uri,
        headers: HeaderMap,
        body: Bytes,
        l2_subgraph_id: Option<SubgraphId>,
    ) -> Response<Body> {
        // We originally attempted to proxy the user's request, but that resulted in a lot of strange
        // behavior from Cloudflare that was too difficult to debug.
        let l2_path = l2_request_path(original_path, l2_subgraph_id);
        let l2_url = l2_url.join(&l2_path).unwrap();
        tracing::info!(%l2_url, %original_path);
        let headers = headers
            .clone()
            .into_iter()
            .filter_map(|(k, v)| Some((k?, v)))
            .filter(|(k, _)| {
                [header::CONTENT_TYPE, header::AUTHORIZATION, header::ORIGIN].contains(k)
            })
            .collect();
        let response = match self
            .http_client
            .post(l2_url)
            .headers(headers)
            .body(body.to_owned())
            .send()
            .await
            .and_then(|response| response.error_for_status())
        {
            Ok(response) => response,
            Err(err) => return error_response(Error::Internal(anyhow!("L2 gateway error: {err}"))),
        };
        let status = response.status();
        if !status.is_success() {
            return error_response(Error::Internal(anyhow!("L2 gateway error: {status}")));
        }
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(err) => return error_response(Error::Internal(anyhow!("L2 gateway error: {err}"))),
        };

        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap()
    }

    async fn parse_request(
        &self,
        context: &GatewayRequestContext,
        request: GatewayRequest,
    ) -> Result<Self::Request, Error> {
        let body: Query = serde_json::from_reader(request.body.reader())
            .map_err(|err| Error::BadQuery(anyhow!("failed to parse request body: {err}")))?;

        let variables = body
            .variables
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        let agora_context = AgoraContext::new_static(&body.query, &variables)
            .map_err(|err| Error::BadQuery(anyhow!("{err}")))?;

        tracing::info!(
            target: CLIENT_REQUEST_TARGET,
            query = %body.query,
            %variables,
        );

        Ok(SubgraphRequest { agora_context })
    }

    fn deterministic_request(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
        chain: &Chain,
        block_requirements: &BlockRequirements,
        blocks_behind: u64,
    ) -> Result<Self::DeterministicRequest, Error> {
        Ok(DeterministicSubgraphRequest {})
    }

    async fn block_constraints(
        &self,
        context: &GatewayRequestContext,
        request: &Self::Request,
    ) -> Result<BTreeSet<BlockConstraint>, Error> {
        let mut constraints = BTreeSet::new();

        let variables = &request.agora_context.variables;
        let operations = &request.agora_context.operations;

        // ba6c90f1-3baf-45be-ac1c-f60733404436
        for operation in operations {
            let (selection_set, defaults) = match operation {
                OperationDefinition::SelectionSet(selection_set) => {
                    (selection_set, BTreeMap::default())
                }
                OperationDefinition::Query(query) if query.directives.is_empty() => {
                    // Add default definitions for variables not set at top level.
                    let defaults = query
                        .variable_definitions
                        .iter()
                        .filter(|d| !variables.0.contains_key(&d.name))
                        .filter_map(|d| {
                            Some((d.name.clone(), d.default_value.as_ref()?.to_graphql()))
                        })
                        .collect::<BTreeMap<String, StaticValue>>();
                    (&query.selection_set, defaults)
                }
                OperationDefinition::Query(_)
                | OperationDefinition::Mutation(_)
                | OperationDefinition::Subscription(_) => {
                    return Err(Error::BadQuery(anyhow!("unsupported GraphQL features")))
                }
            };
            for selection in &selection_set.items {
                let selection_field = match selection {
                    Selection::Field(field) => field,
                    Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                        return Err(Error::BadQuery(anyhow!("unsupported GraphQL features")))
                    }
                };
                let constraint = match selection_field
                    .arguments
                    .iter()
                    .find(|(k, _)| *k == "block")
                {
                    Some((_, arg)) => {
                        field_constraint(&variables, &defaults, arg).map_err(Error::BadQuery)?
                    }
                    None => BlockConstraint::Unconstrained,
                };
                constraints.insert(constraint);
            }
        }
        Ok(constraints)
    }

    async fn indexer_request_fee(
        &self,
        context: &GatewayRequestContext,
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
        context: &GatewayRequestContext,
        selection: &SelectionInfo,
        request: Self::DeterministicRequest,
    ) -> Result<IndexerResponse, IndexerError> {
        todo!()
    }

    async fn finalize_response(
        &self,
        result: Result<(SelectionInfo, IndexerResponse), Error>,
    ) -> Response<Body> {
        match result {
            Ok((selection, response)) => Response::builder()
                .status(StatusCode::OK)
                .header_typed(ContentType::json())
                .header_typed(GraphAttestation(response.attestation))
                .body(response.body)
                .unwrap(),
            Err(err) => error_response(err),
        }
    }
}

fn l2_request_path(original_path: &Uri, l2_subgraph_id: Option<SubgraphId>) -> String {
    let mut path = original_path.path().to_string();
    let subgraph_prefix = "subgraphs/id/";
    let subgraph_start = path.find(subgraph_prefix);
    // rewrite path of subgraph queries to the L2 subgraph ID, conserving version constraint
    if let (Some(l2_subgraph_id), Some(replace_start)) = (l2_subgraph_id, subgraph_start) {
        let replace_start = replace_start + subgraph_prefix.len();
        let replace_end = path.find('^').unwrap_or(path.len());
        path.replace_range(replace_start..replace_end, &l2_subgraph_id.to_string());
    }
    path
}

#[cfg(test)]
mod tests {
    use thegraph_core::types::{DeploymentId, SubgraphId};

    use super::l2_request_path;

    #[test]
    fn test_l2_request_path() {
        let deployment: DeploymentId = "QmdveVMs7nAvdBPxNoaMMAYgNcuSroneMctZDnZUgbPPP3"
            .parse()
            .unwrap();
        let l1_subgraph: SubgraphId = "EMRitnR1t3drKrDQSmJMSmHBPB2sGotgZE12DzWNezDn"
            .parse()
            .unwrap();
        let l2_subgraph: SubgraphId = "CVHoVSrdiiYvLcH4wocDCazJ1YuixHZ1SKt34UWmnQcC"
            .parse()
            .unwrap();

        // test deployment route
        let mut original = format!("/api/deployments/id/{deployment}").parse().unwrap();
        let mut expected = format!("/api/deployments/id/{deployment}");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));

        // test subgraph route
        original = format!("/api/subgraphs/id/{l1_subgraph}").parse().unwrap();
        expected = format!("/api/subgraphs/id/{l2_subgraph}");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));

        // test subgraph route with API key prefix
        original = format!("/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{l1_subgraph}")
            .parse()
            .unwrap();
        expected = format!("/api/deadbeefdeadbeefdeadbeefdeadbeef/subgraphs/id/{l2_subgraph}");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));

        // test subgraph route with version constraint
        original = format!("/api/subgraphs/id/{l1_subgraph}^0.0.1")
            .parse()
            .unwrap();
        expected = format!("/api/subgraphs/id/{l2_subgraph}^0.0.1");
        assert_eq!(expected, l2_request_path(&original, Some(l2_subgraph)));
    }
}
