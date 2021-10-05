use crate::{
    indexer_selection::{
        self, CostModelSource, IndexerDataReader, IndexerDataWriter, Indexing, IndexingData,
        SecretKey, SelectionFactors,
    },
    prelude::{shared_lookup::SharedLookupWriter, *},
    query_engine::{APIKey, InputWriters},
};
use eventuals::EventualExt as _;
use graphql_client::{GraphQLQuery, Response};
use im;
use prometheus;
use reqwest;
use serde_json::{json, Value as JSON};
use std::{collections::HashMap, iter::FromIterator, sync::Arc};
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};
use tracing::{self, Instrument};
use uuid::Uuid;

#[derive(Clone)]
pub struct Metrics {
    pub allocations: prometheus::IntGauge,
    pub transfers: prometheus::IntGauge,
}

pub fn create(
    agent_url: String,
    poll_interval: Duration,
    gateway_id: Uuid,
    signer_key: SecretKey,
    inputs: InputWriters,
    api_keys: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
) -> Metrics {
    let _trace = tracing::info_span!("sync client", ?poll_interval).entered();
    let InputWriters {
        indexer_inputs:
            indexer_selection::InputWriters {
                slashing_percentage,
                usd_to_grt_conversion,
                indexers,
                indexings,
            },
        deployments,
        deployment_indexers,
        indexers: indexer_selection,
    } = inputs;
    let indexings = Arc::new(Mutex::new(indexings));

    let metrics = Metrics {
        allocations: prometheus::register_int_gauge!("allocations", "Total allocations").unwrap(),
        transfers: prometheus::register_int_gauge!("transfers", "Total transfers").unwrap(),
    };

    create_sync_client::<APIKeys, _>(
        gateway_id,
        agent_url.clone(),
        poll_interval,
        api_keys::OPERATION_NAME,
        api_keys::QUERY,
        parse_api_keys,
        api_keys,
    );
    create_sync_client::<ConversionRates, _>(
        gateway_id,
        agent_url.clone(),
        poll_interval,
        conversion_rates::OPERATION_NAME,
        conversion_rates::QUERY,
        parse_conversion_rates,
        usd_to_grt_conversion,
    );
    create_sync_client::<NetworkParameters, _>(
        gateway_id,
        agent_url.clone(),
        poll_interval,
        network_parameters::OPERATION_NAME,
        network_parameters::QUERY,
        parse_network_parameters,
        slashing_percentage,
    );
    handle_cost_models(
        indexings.clone(),
        create_sync_client_input::<CostModels, _>(
            gateway_id,
            agent_url.clone(),
            poll_interval,
            cost_models::OPERATION_NAME,
            cost_models::QUERY,
            parse_cost_models,
        ),
    );
    handle_deployments(
        indexers,
        deployments,
        deployment_indexers,
        create_sync_client_input::<CurrentDeployments, _>(
            gateway_id,
            agent_url.clone(),
            poll_interval,
            current_deployments::OPERATION_NAME,
            current_deployments::QUERY,
            parse_current_deployments,
        ),
        create_sync_client_input::<Indexers, _>(
            gateway_id,
            agent_url.clone(),
            poll_interval,
            indexers::OPERATION_NAME,
            indexers::QUERY,
            parse_indexers,
        ),
    );
    handle_indexing_statuses(
        indexer_selection.clone(),
        create_sync_client_input::<IndexingStatuses, _>(
            gateway_id,
            agent_url.clone(),
            poll_interval,
            indexing_statuses::OPERATION_NAME,
            indexing_statuses::QUERY,
            parse_indexing_statuses,
        ),
    );
    handle_transfers(
        indexer_selection.clone(),
        metrics.clone(),
        create_sync_client_input::<Transfers, _>(
            gateway_id,
            agent_url.clone(),
            poll_interval,
            transfers::OPERATION_NAME,
            transfers::QUERY,
            parse_transfers,
        ),
    );
    handle_allocations(
        indexer_selection.clone(),
        signer_key,
        metrics.clone(),
        create_sync_client_input::<UsableAllocations, _>(
            gateway_id,
            agent_url.clone(),
            poll_interval,
            usable_allocations::OPERATION_NAME,
            usable_allocations::QUERY,
            parse_usable_allocations,
        ),
    );
    metrics
}

fn create_sync_client_input<Q, T>(
    gateway_id: Uuid,
    agent_url: String,
    poll_interval: Duration,
    operation: &'static str,
    query: &'static str,
    parse_data: fn(Q::ResponseData) -> Option<T>,
) -> Eventual<T>
where
    T: 'static + Clone + Eq + Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static,
{
    let (writer, reader) = Eventual::new();
    create_sync_client::<Q, T>(
        gateway_id,
        agent_url,
        poll_interval,
        operation,
        query,
        parse_data,
        writer,
    );
    reader
}

fn create_sync_client<Q, T>(
    gateway_id: Uuid,
    agent_url: String,
    poll_interval: Duration,
    operation: &'static str,
    query: &'static str,
    parse_data: fn(Q::ResponseData) -> Option<T>,
    mut writer: EventualWriter<T>,
) where
    T: 'static + Clone + Eq + Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static,
{
    tokio::spawn(
        async move {
            let client = reqwest::Client::new();
            let mut last_update_id = "<none>".to_string();
            loop {
                if let Some(data) = execute_query::<Q, T>(
                    gateway_id,
                    &agent_url,
                    operation,
                    query,
                    parse_data,
                    &client,
                    &mut last_update_id,
                )
                .in_current_span()
                .await
                {
                    writer.write(data);
                }
                sleep(poll_interval).await;
            }
        }
        .instrument(tracing::info_span!("poller", query = operation)),
    );
}

async fn execute_query<'f, Q, T>(
    uuid: Uuid,
    agent_url: &'f str,
    operation: &'static str,
    query: &'static str,
    parse_data: fn(Q::ResponseData) -> Option<T>,
    client: &'f reqwest::Client,
    last_update_id: &'f mut String,
) -> Option<T>
where
    T: 'static + Clone + Eq + Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static,
{
    // TODO: Don't use graphql_client and and just rely on graphql-parser. This is a bit more trouble than it's worth.
    let variables = if operation == transfers::OPERATION_NAME {
        json!({"lastUpdateId": last_update_id, "gatewayId": uuid.to_string()})
    } else {
        json!({"lastUpdateId": last_update_id, })
    };
    let body = json!({
        "operationName": operation,
        "query": query,
        "variables": variables,
    });
    tracing::trace!(%operation, op = %transfers::OPERATION_NAME, %variables);
    let response_start = match client.post(agent_url).json(&body).send().await {
        Ok(response_start) => response_start,
        Err(query_err) => {
            tracing::error!(%query_err);
            return None;
        }
    };
    let response_raw = match response_start.text().await {
        Ok(text) => text,
        Err(query_err) => {
            tracing::error!(%query_err);
            return None;
        }
    };
    tracing::trace!(%response_raw);
    let response_data = match serde_json::from_str::<JSON>(&response_raw) {
        Ok(response_json) => response_json
            .get("data")
            .and_then(|data| data.get("data"))
            .map(JSON::to_owned),
        Err(err) => {
            tracing::error!(%err, "response is invalid JSON");
            return None;
        }
    };
    if response_data.as_ref().map(JSON::is_null).unwrap_or(false) {
        tracing::debug!("up to date");
    } else if let Some(update_id) = response_data
        .as_ref()
        .and_then(|data| Some(data.get("updateId")?.as_str()?))
    {
        *last_update_id = update_id.to_string();
        tracing::debug!(update_id = %last_update_id);
    } else {
        tracing::warn!("updateId not found in {} response", operation);
    }
    let response: Response<Q::ResponseData> = match serde_json::from_str(&response_raw) {
        Ok(response) => response,
        Err(query_response_parse_err) => {
            tracing::error!(%query_response_parse_err);
            return None;
        }
    };
    if let Some(errs) = response.errors {
        if !errs.is_empty() {
            tracing::error!(query_response_errors = %format!("{:?}", errs));
            return None;
        }
    }
    if let Some(data) = response.data.map(parse_data) {
        return data;
    }
    tracing::error!("malformed response data");
    None
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/api_keys.gql",
    response_derives = "Debug"
)]
struct APIKeys;

fn parse_api_keys(data: api_keys::ResponseData) -> Option<Ptr<HashMap<String, Arc<APIKey>>>> {
    match data {
        api_keys::ResponseData {
            data: Some(api_keys::ApiKeysData { value, .. }),
        } => {
            let parsed = value
                .into_iter()
                .filter_map(|value| {
                    Some(APIKey {
                        key: value.api_key.key,
                        user_address: value.user.eth_address.parse().ok()?,
                        queries_activated: value.user.queries_activated,
                        deployments: value
                            .deployments
                            .into_iter()
                            .filter_map(|id| SubgraphDeploymentID::from_ipfs_hash(&id))
                            .collect(),
                        domains: value
                            .domains
                            .into_iter()
                            .map(|domain| domain.name)
                            .collect(),
                    })
                })
                .collect::<Vec<APIKey>>();
            tracing::info!(api_keys = %parsed.len());
            Some(Ptr::new(HashMap::from_iter(
                parsed.into_iter().map(|v| (v.key.clone(), Arc::new(v))),
            )))
        }
        _ => None,
    }
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/conversion_rates.gql",
    response_derives = "Debug"
)]
struct ConversionRates;

fn parse_conversion_rates(data: conversion_rates::ResponseData) -> Option<USD> {
    use conversion_rates::{ConversionRatesData, ConversionRatesDataValue};
    match data {
        conversion_rates::ResponseData {
            data:
                Some(ConversionRatesData {
                    value:
                        ConversionRatesDataValue {
                            grt_to_dai: Some(usd_per_grt),
                            ..
                        },
                    ..
                }),
        } => {
            tracing::info!(?usd_per_grt);
            usd_per_grt.parse::<USD>().ok()
        }
        _ => None,
    }
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/cost_models.gql",
    response_derives = "Debug"
)]
struct CostModels;

fn parse_cost_models(
    data: cost_models::ResponseData,
) -> Option<Ptr<Vec<(Indexing, CostModelSource)>>> {
    use cost_models::{CostModelsData, ResponseData};
    let values = match data {
        ResponseData {
            data: Some(CostModelsData { value, .. }),
        } => value,
        _ => return None,
    };
    let parsed = values
        .into_iter()
        .flat_map(|value| {
            let deployment = SubgraphDeploymentID::from_ipfs_hash(&value.deployment);
            value.cost_models.into_iter().filter_map(move |model| {
                Some((
                    Indexing {
                        subgraph: deployment?,
                        indexer: model.indexer.id.parse().ok()?,
                    },
                    CostModelSource {
                        model: model.model?,
                        globals: model.variables.unwrap_or_default(),
                    },
                ))
            })
        })
        .collect();
    Some(Ptr::new(parsed))
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/current_deployments.gql",
    response_derives = "Debug"
)]
struct CurrentDeployments;

fn parse_current_deployments(
    data: current_deployments::ResponseData,
) -> Option<Ptr<Vec<(String, SubgraphDeploymentID)>>> {
    use current_deployments::{CurrentDeploymentsData, ResponseData};
    let values = match data {
        ResponseData {
            data: Some(CurrentDeploymentsData { value, .. }),
        } => value,
        _ => return None,
    };
    let parsed = values
        .into_iter()
        .filter_map(|value| {
            Some((
                value.subgraph,
                SubgraphDeploymentID::from_ipfs_hash(&value.deployment)?,
            ))
        })
        .collect();
    Some(Ptr::new(parsed))
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/indexers.gql",
    response_derives = "Debug"
)]
struct Indexers;

#[derive(Clone, Eq, PartialEq)]
struct ParsedIndexerInfo {
    id: Address,
    url: String,
    staked: GRT,
    delegated: GRT,
}

fn parse_indexers(
    data: indexers::ResponseData,
) -> Option<Ptr<Vec<(SubgraphDeploymentID, Vec<ParsedIndexerInfo>)>>> {
    use indexers::{IndexersData, IndexersDataValue, ResponseData};
    let value = match data {
        ResponseData {
            data: Some(IndexersData { value, .. }),
        } => value,
        _ => return None,
    };
    fn parse_value(
        value: IndexersDataValue,
    ) -> Option<(SubgraphDeploymentID, Vec<ParsedIndexerInfo>)> {
        let deployment = SubgraphDeploymentID::from_ipfs_hash(&value.deployment)?;
        let indexers = value
            .indexers
            .into_iter()
            .filter_map(|indexer| {
                Some(ParsedIndexerInfo {
                    id: indexer.id.parse::<Address>().ok()?,
                    // TODO: parse URL
                    url: indexer.url.trim_end_matches('/').into(),
                    staked: indexer.staked_tokens.parse().ok()?,
                    delegated: indexer.delegated_tokens.parse().ok()?,
                })
            })
            .collect();
        Some((deployment, indexers))
    }
    Some(Ptr::new(
        value.into_iter().filter_map(parse_value).collect(),
    ))
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/indexing_statuses.gql",
    response_derives = "Debug"
)]
struct IndexingStatuses;

#[derive(Clone, Eq, PartialEq)]
struct ParsedIndexingStatus {
    indexing: Indexing,
    network: String,
    block_number: Option<u64>,
}

fn parse_indexing_statuses(
    data: indexing_statuses::ResponseData,
) -> Option<Ptr<Vec<ParsedIndexingStatus>>> {
    use indexing_statuses::{IndexingStatusesData, ResponseData};
    let values = match data {
        ResponseData {
            data: Some(IndexingStatusesData { value, .. }),
        } => value,
        _ => return None,
    };
    let parsed = values
        .into_iter()
        .flat_map(|value| {
            let subgraph = SubgraphDeploymentID::from_ipfs_hash(&value.deployment);
            value.statuses.into_iter().filter_map(move |status| {
                Some(ParsedIndexingStatus {
                    indexing: Indexing {
                        subgraph: subgraph?,
                        indexer: status.indexer.id.parse().ok()?,
                    },
                    network: status.network,
                    block_number: status.block.and_then(|b| b.hash.parse().ok()),
                })
            })
        })
        .collect();
    Some(Ptr::new(parsed))
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/network_parameters.gql",
    response_derives = "Debug"
)]
struct NetworkParameters;

fn parse_network_parameters(data: network_parameters::ResponseData) -> Option<PPM> {
    use network_parameters::{NetworkParametersData, ResponseData};
    match data {
        ResponseData {
            data: Some(NetworkParametersData { value, .. }),
        } => {
            let slashing_percentage = value.slashing_percentage.to_string().parse::<PPM>().ok()?;
            tracing::info!(?slashing_percentage);
            Some(slashing_percentage)
        }
        _ => None,
    }
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/transfers.gql",
    response_derives = "Debug"
)]
struct Transfers;

#[derive(Clone, Eq, PartialEq)]
struct ParsedTransfer {
    id: Bytes32,
    indexing: Indexing,
    collateral: GRT,
    signer_key: SecretKey,
}

fn parse_transfers(data: transfers::ResponseData) -> Option<Ptr<Vec<ParsedTransfer>>> {
    use transfers::{ResponseData, TransfersData};
    let transfers = match data {
        ResponseData {
            data: Some(TransfersData { value, .. }),
        } => value,
        _ => return None,
    };
    let parsed = transfers
        .into_iter()
        .filter_map(|value| {
            Some(ParsedTransfer {
                id: value.id.parse().ok()?,
                indexing: Indexing {
                    subgraph: SubgraphDeploymentID::from_ipfs_hash(&value.deployment)?,
                    indexer: value.indexer.id.parse().ok()?,
                },
                collateral: value.collateral.parse().ok()?,
                signer_key: value.signer_key.parse().ok()?,
            })
        })
        .collect();
    Some(Ptr::new(parsed))
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/usable_allocations.gql",
    response_derives = "Debug"
)]
struct UsableAllocations;

#[derive(Clone, Eq, PartialEq)]
struct ParsedAllocation {
    id: Address,
    indexing: Indexing,
}

fn parse_usable_allocations(
    data: usable_allocations::ResponseData,
) -> Option<Ptr<Vec<ParsedAllocation>>> {
    use usable_allocations::{ResponseData, UsableAllocationsData};
    let usable_allocations = match data {
        ResponseData {
            data: Some(UsableAllocationsData { value, .. }),
        } => value,
        _ => return None,
    };
    let parsed = usable_allocations
        .into_iter()
        .filter_map(|value| {
            Some(ParsedAllocation {
                id: value.id.parse().ok()?,
                indexing: Indexing {
                    subgraph: SubgraphDeploymentID::from_ipfs_hash(&value.subgraph_deployment_id)?,
                    indexer: value.indexer.id.parse().ok()?,
                },
            })
        })
        .collect();
    Some(Ptr::new(parsed))
}

fn handle_cost_models(
    indexings: Arc<Mutex<SharedLookupWriter<Indexing, SelectionFactors, IndexingData>>>,
    cost_models: Eventual<Ptr<Vec<(Indexing, CostModelSource)>>>,
) {
    cost_models
        .pipe_async(move |cost_models| {
            let indexings = indexings.clone();
            async move {
                tracing::info!(cost_models = %cost_models.len());
                let mut locked = indexings.lock().await;
                for (indexing, model) in cost_models.iter() {
                    let writer = locked.write(&indexing).await;
                    writer.cost_model.write(model.clone());
                }
            }
            .instrument(tracing::info_span!("handle_cost_models"))
        })
        .forever();
}

fn handle_deployments(
    indexers: SharedLookupWriter<Address, IndexerDataReader, IndexerDataWriter>,
    mut deployments: EventualWriter<Ptr<HashMap<String, SubgraphDeploymentID>>>,
    mut deployment_indexers: EventualWriter<
        Ptr<HashMap<SubgraphDeploymentID, im::Vector<Address>>>,
    >,
    current_deployments: Eventual<Ptr<Vec<(String, SubgraphDeploymentID)>>>,
    indexer_statuses: Eventual<Ptr<Vec<(SubgraphDeploymentID, Vec<ParsedIndexerInfo>)>>>,
) {
    let indexers = Arc::new(Mutex::new(indexers));
    eventuals::join((current_deployments, indexer_statuses))
        .pipe_async(move |(current_deployments, indexer_statuses)| {
            let _span = tracing::info_span!("handle_deployments").entered();
            tracing::info!(
                current_deployments = %current_deployments.len(),
                indexed_deployments = %indexer_statuses.len(),
            );
            deployments.write(Ptr::new(current_deployments.iter().cloned().collect()));
            deployment_indexers.write(Ptr::new(
                indexer_statuses
                    .iter()
                    .map(|(deployment, indexer_statuses)| {
                        // TODO: We are assuming the network is mainnet for now.
                        let network = "mainnet".to_string();
                        tracing::trace!(
                            %network,
                            ?deployment,
                            indexer_statuses = indexer_statuses.len(),
                        );
                        (
                            deployment.clone(),
                            indexer_statuses.iter().map(|status| status.id).collect(),
                        )
                    })
                    .collect(),
            ));
            let statuses =
                HashMap::<Address, ParsedIndexerInfo>::from_iter(indexer_statuses.iter().flat_map(
                    |(_, statuses)| statuses.iter().cloned().map(|status| (status.id, status)),
                ));
            tracing::info!(indexers = %statuses.len());
            let indexers = indexers.clone();
            async move {
                let mut indexers = indexers.lock().await;
                for (indexer, status) in statuses {
                    let indexer = indexers.write(&indexer).await;
                    indexer.url.write(status.url);
                    indexer.stake.write(status.staked);
                    indexer.delegated_stake.write(status.delegated);
                }
            }
        })
        .forever();
}

fn handle_indexing_statuses(
    indexer_selection: Arc<indexer_selection::Indexers>,
    indexing_statuses: Eventual<Ptr<Vec<ParsedIndexingStatus>>>,
) {
    indexing_statuses
        .pipe_async(move |indexing_statuses| {
            let indexer_selection = indexer_selection.clone();
            async move {
                tracing::info!(indexing_statuses = %indexing_statuses.len());
                for status in indexing_statuses.iter() {
                    indexer_selection
                        .set_indexing_status(
                            &status.network,
                            &status.indexing,
                            status.block_number.unwrap_or(0),
                        )
                        .await;
                }
            }
            .instrument(tracing::info_span!("handle_indexing_statuses"))
        })
        .forever();
}

fn handle_transfers(
    indexer_selection: Arc<indexer_selection::Indexers>,
    metrics: Metrics,
    transfers: Eventual<Ptr<Vec<ParsedTransfer>>>,
) {
    let mut used_transfers = Ptr::<Vec<ParsedTransfer>>::default();
    transfers
        .pipe_async(move |transfers| {
            let used_transfers = std::mem::replace(&mut used_transfers, transfers.clone());
            let indexer_selection = indexer_selection.clone();
            let metrics = metrics.clone();
            async move {
                tracing::info!(transfers = %transfers.len());
                metrics.transfers.set(transfers.len() as i64);
                // Add new transfers.
                for transfer in transfers.iter() {
                    if used_transfers.iter().any(|t| t.id == transfer.id) {
                        continue;
                    }
                    indexer_selection
                        .install_receipts_transfer(
                            &transfer.indexing,
                            transfer.id,
                            &transfer.collateral,
                            transfer.signer_key,
                        )
                        .await;
                }
                // Remove old transfers.
                for transfer in used_transfers.iter() {
                    if transfers.iter().any(|t| t.id == transfer.id) {
                        continue;
                    }
                    // HOY (Hack Of the Year): Remove the old transfer in 5 minutes, to give new
                    // allocations and replacement transfer some time to propagate through the
                    // system and into indexer selection.
                    let indexer_selection = indexer_selection.clone();
                    let transfer = transfer.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_secs(5 * 60)).await;
                        indexer_selection
                            .remove_receipts_transfer(&transfer.indexing, &transfer.id)
                            .await;
                    });
                }
            }
            .instrument(tracing::info_span!("handle_transfers"))
        })
        .forever();
}

fn handle_allocations(
    indexer_selection: Arc<indexer_selection::Indexers>,
    signer_key: SecretKey,
    metrics: Metrics,
    allocations: Eventual<Ptr<Vec<ParsedAllocation>>>,
) {
    let mut used_allocations = Ptr::<Vec<ParsedAllocation>>::default();
    allocations
        .pipe_async(move |allocations| {
            let used_allocations = std::mem::replace(&mut used_allocations, allocations.clone());
            let indexer_selection = indexer_selection.clone();
            let metrics = metrics.clone();
            async move {
                tracing::info!(allocations = %allocations.len());
                metrics.allocations.set(allocations.len() as i64);
                // Add new allocations.
                for allocation in allocations.iter() {
                    if used_allocations.iter().any(|t| t.id == allocation.id) {
                        continue;
                    }
                    indexer_selection
                        .install_receipts_allocation(
                            &allocation.indexing,
                            allocation.id,
                            signer_key,
                        )
                        .await;
                }
                // Remove old allocations.
                for allocation in used_allocations.iter() {
                    if allocations.iter().any(|t| t.id == allocation.id) {
                        continue;
                    }
                    // HOY (Hack Of the Year): Remove the old allocation in 5 minutes, to give new
                    // allocations and replacement transfer some time to propagate through the
                    // system and into indexer selection.
                    let indexer_selection = indexer_selection.clone();
                    let allocation = allocation.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_secs(5 * 60)).await;
                        indexer_selection
                            .remove_receipts_allocation(&allocation.indexing, &allocation.id)
                            .await;
                    });
                }
            }
            .instrument(tracing::info_span!("handle_allocations"))
        })
        .forever();
}
