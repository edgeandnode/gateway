use crate::{
    indexer_selection::{
        self, CostModelSource, IndexerDataReader, IndexerDataWriter, Indexing, IndexingData,
        SecretKey, SelectionFactors,
    },
    prelude::{shared_lookup::SharedLookupWriter, *},
    query_engine::InputWriters,
};
use graphql_client::{GraphQLQuery, Response};
use im;
use reqwest;
use serde_json::{json, Value as JSON};
use std::{collections::HashMap, iter::FromIterator, sync::Arc};
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};
use tracing::{self, Instrument};
use uuid::Uuid;

pub fn create(
    agent_url: String,
    poll_interval: Duration,
    signer_key: SecretKey,
    inputs: InputWriters,
) {
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

    let gateway_id = Uuid::new_v4();

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
        create_sync_client_input::<UsableAllocations, _>(
            gateway_id,
            agent_url.clone(),
            poll_interval,
            usable_allocations::OPERATION_NAME,
            usable_allocations::QUERY,
            parse_usable_allocations,
        ),
    );
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

fn parse_cost_models(data: cost_models::ResponseData) -> Option<Vec<(Indexing, CostModelSource)>> {
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
    Some(parsed)
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
) -> Option<im::Vector<(String, SubgraphDeploymentID)>> {
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
    Some(parsed)
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/sync_agent_schema.gql",
    query_path = "graphql/indexers.gql",
    response_derives = "Debug"
)]
struct Indexers;

#[derive(Clone, Eq, PartialEq)]
struct ParsedIndexerStatus {
    id: Address,
    staked: GRT,
    delegated: GRT,
}

fn parse_indexers(
    data: indexers::ResponseData,
) -> Option<im::Vector<(SubgraphDeploymentID, im::Vector<ParsedIndexerStatus>)>> {
    use indexers::{IndexersData, IndexersDataValue, ResponseData};
    let value = match data {
        ResponseData {
            data: Some(IndexersData { value, .. }),
        } => value,
        _ => return None,
    };
    fn parse_value(
        value: IndexersDataValue,
    ) -> Option<(SubgraphDeploymentID, im::Vector<ParsedIndexerStatus>)> {
        let deployment = SubgraphDeploymentID::from_ipfs_hash(&value.deployment)?;
        let indexers = value
            .indexers
            .into_iter()
            .filter_map(|indexer| {
                Some(ParsedIndexerStatus {
                    id: indexer.id.parse::<Address>().ok()?,
                    staked: indexer.staked_tokens.parse().ok()?,
                    delegated: indexer.delegated_tokens.parse().ok()?,
                })
            })
            .collect();
        Some((deployment, indexers))
    }
    Some(value.into_iter().filter_map(parse_value).collect())
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
) -> Option<im::Vector<ParsedIndexingStatus>> {
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
        .collect::<im::Vector<ParsedIndexingStatus>>();
    Some(parsed)
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

fn parse_transfers(data: transfers::ResponseData) -> Option<im::Vector<ParsedTransfer>> {
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
    Some(parsed)
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
) -> Option<im::Vector<ParsedAllocation>> {
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
    Some(parsed)
}

#[tracing::instrument(skip(indexings, cost_models))]
fn handle_cost_models(
    indexings: Arc<Mutex<SharedLookupWriter<Indexing, SelectionFactors, IndexingData>>>,
    cost_models: Eventual<Vec<(Indexing, CostModelSource)>>,
) {
    tokio::spawn(
        async move {
            let mut cost_models = cost_models.subscribe();
            loop {
                let cost_models = match cost_models.next().await {
                    Ok(cost_models) => cost_models,
                    Err(_) => break,
                };
                tracing::info!(cost_models = %cost_models.len());
                let mut locked = indexings.lock().await;
                for (indexing, model) in cost_models {
                    let writer = locked.write(&indexing).await;
                    writer.cost_model.write(model);
                }
            }
        }
        .in_current_span(),
    );
}

#[tracing::instrument(skip(
    indexers,
    deployments,
    deployment_indexers,
    current_deployments,
    indexer_statuses
))]
fn handle_deployments(
    mut indexers: SharedLookupWriter<Address, IndexerDataReader, IndexerDataWriter>,
    mut deployments: EventualWriter<im::HashMap<String, SubgraphDeploymentID>>,
    mut deployment_indexers: EventualWriter<im::HashMap<SubgraphDeploymentID, im::Vector<Address>>>,
    current_deployments: Eventual<im::Vector<(String, SubgraphDeploymentID)>>,
    indexer_statuses: Eventual<im::Vector<(SubgraphDeploymentID, im::Vector<ParsedIndexerStatus>)>>,
) {
    tokio::spawn(
        async move {
            let mut update = eventuals::join((current_deployments, indexer_statuses)).subscribe();
            loop {
                let (current_deployments, indexer_statuses) = match update.next().await {
                    Ok(update) => update,
                    Err(_) => break,
                };
                tracing::info!(
                    current_deployments = %current_deployments.len(),
                    indexed_deployments = %indexer_statuses.len(),
                );
                deployments.write(current_deployments.into_iter().collect());
                deployment_indexers.write(
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
                );
                let statuses = HashMap::<Address, ParsedIndexerStatus>::from_iter(
                    indexer_statuses.into_iter().flat_map(|(_, statuses)| {
                        statuses.into_iter().map(|status| (status.id, status))
                    }),
                );
                tracing::info!(indexers = %statuses.len());
                for (indexer, status) in statuses {
                    let indexer = indexers.write(&indexer).await;
                    indexer.stake.write(status.staked);
                    indexer.delegated_stake.write(status.delegated);
                }
            }
        }
        .in_current_span(),
    );
}

#[tracing::instrument(skip(indexer_selection, indexing_statuses))]
fn handle_indexing_statuses(
    indexer_selection: Arc<indexer_selection::Indexers>,
    indexing_statuses: Eventual<im::Vector<ParsedIndexingStatus>>,
) {
    tokio::spawn(
        async move {
            let mut indexing_statuses = indexing_statuses.subscribe();
            loop {
                let indexing_statuses = match indexing_statuses.next().await {
                    Ok(indexing_statuses) => indexing_statuses,
                    Err(_) => break,
                };
                tracing::info!(indexing_statuses = %indexing_statuses.len());
                for status in indexing_statuses {
                    if let Some(block_number) = status.block_number {
                        indexer_selection
                            .set_indexing_status(&status.network, &status.indexing, block_number)
                            .await;
                    }
                }
            }
        }
        .in_current_span(),
    );
}

#[tracing::instrument(skip(indexer_selection, transfers))]
fn handle_transfers(
    indexer_selection: Arc<indexer_selection::Indexers>,
    transfers: Eventual<im::Vector<ParsedTransfer>>,
) {
    let mut used_transfers = im::Vector::<ParsedTransfer>::new();
    tokio::spawn(
        async move {
            let mut transfers = transfers.subscribe();
            loop {
                let transfers = match transfers.next().await {
                    Ok(transfers) => transfers,
                    Err(_) => break,
                };
                tracing::info!(transfers = %transfers.len());
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
                used_transfers = transfers;
            }
        }
        .in_current_span(),
    );
}

#[tracing::instrument(skip(indexer_selection, signer_key, allocations))]
fn handle_allocations(
    indexer_selection: Arc<indexer_selection::Indexers>,
    signer_key: SecretKey,
    allocations: Eventual<im::Vector<ParsedAllocation>>,
) {
    let mut used_allocations = im::Vector::<ParsedAllocation>::new();
    tokio::spawn(
        async move {
            let mut allocations = allocations.subscribe();
            loop {
                let allocations = match allocations.next().await {
                    Ok(allocations) => allocations,
                    Err(_) => break,
                };
                tracing::info!(allocations = %allocations.len());
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
                    indexer_selection
                        .remove_receipts_allocation(&allocation.indexing, &allocation.id)
                        .await;
                }
                used_allocations = allocations;
            }
        }
        .in_current_span(),
    );
}
