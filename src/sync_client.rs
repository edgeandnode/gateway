use crate::{
    indexer_selection::{
        self, CostModelSource, IndexerDataReader, IndexerDataWriter, Indexing, IndexingData,
        SecretKey, SelectionFactors,
    },
    prelude::{shared_lookup::SharedLookupWriter, *},
    query_engine::{Deployment, DeploymentWriter, InputWriters, QualifiedSubgraph},
};
use graphql_client::{GraphQLQuery, Response};
use im;
use regex::Regex;
use reqwest;
use serde_json::json;
use std::{collections::HashMap, iter::FromIterator, sync::Arc};
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};
use tracing::{self, Instrument};

pub fn create(agent_url: String, poll_interval: Duration, inputs: InputWriters) {
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
        indexers: indexer_selection,
    } = inputs;
    let indexings = Arc::new(Mutex::new(indexings));

    create_sync_client::<ConversionRates, _>(
        agent_url.clone(),
        poll_interval,
        conversion_rates::OPERATION_NAME,
        conversion_rates::QUERY,
        parse_conversion_rates,
        usd_to_grt_conversion,
    );
    create_sync_client::<NetworkParameters, _>(
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
        create_sync_client_input::<CurrentDeployments, _>(
            agent_url.clone(),
            poll_interval,
            current_deployments::OPERATION_NAME,
            current_deployments::QUERY,
            parse_current_deployments,
        ),
        create_sync_client_input::<Indexers, _>(
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
            agent_url.clone(),
            poll_interval,
            indexing_statuses::OPERATION_NAME,
            indexing_statuses::QUERY,
            parse_indexing_statuses,
        ),
    );
}

fn create_sync_client_input<Q, T>(
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
            let update_id_regex = Regex::new(r#"updateId":"([^"]*)""#).unwrap();
            loop {
                if let Some(data) = execute_query::<Q, T>(
                    &agent_url,
                    operation,
                    query,
                    parse_data,
                    &client,
                    &mut last_update_id,
                    &update_id_regex,
                )
                .in_current_span()
                .await
                {
                    writer.write(data);
                }
                sleep(poll_interval).await;
            }
            tracing::error!("{} handler closed", operation);
        }
        .instrument(tracing::info_span!("poller", query = operation)),
    );
}

async fn execute_query<'f, Q, T>(
    agent_url: &'f str,
    operation: &'static str,
    query: &'static str,
    parse_data: fn(Q::ResponseData) -> Option<T>,
    client: &'f reqwest::Client,
    last_update_id: &'f mut String,
    update_id_regex: &'f Regex,
) -> Option<T>
where
    T: 'static + Clone + Eq + Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static,
{
    let body = json!({
        "operationName": operation,
        "query": query,
        "variables": {
           "lastUpdateId": last_update_id,
        }
    });
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
    if let Some(update_id) = update_id_regex
        .captures(&response_raw)
        .and_then(|c| c.get(1))
    {
        *last_update_id = update_id.as_str().to_string();
        tracing::trace!(update_id = %last_update_id);
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

fn parse_cost_models(
    data: cost_models::ResponseData,
) -> Option<im::Vector<(Indexing, CostModelSource)>> {
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
        .collect::<im::Vector<(Indexing, CostModelSource)>>();
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

#[derive(Clone)]
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

// TODO: parse_usable_allocations

#[tracing::instrument(skip(indexings, cost_models))]
fn handle_cost_models(
    indexings: Arc<Mutex<SharedLookupWriter<Indexing, SelectionFactors, IndexingData>>>,
    cost_models: Eventual<im::Vector<(Indexing, CostModelSource)>>,
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

#[tracing::instrument(skip(indexers, deployments, current_deployments, indexer_statuses))]
fn handle_deployments(
    mut indexers: SharedLookupWriter<Address, IndexerDataReader, IndexerDataWriter>,
    mut deployments: SharedLookupWriter<QualifiedSubgraph, Deployment, DeploymentWriter>,
    current_deployments: Eventual<im::Vector<(String, SubgraphDeploymentID)>>,
    indexer_statuses: Eventual<im::Vector<(SubgraphDeploymentID, im::Vector<ParsedIndexerStatus>)>>,
) {
    tokio::spawn(async move {
        let mut update = eventuals::join((current_deployments, indexer_statuses)).subscribe();
        loop {
            let (current_deployments, indexer_statuses) = match update.next().await {
                Ok(update) => update,
                Err(_) => break,
            };
            let deployment_to_subgraph = HashMap::<SubgraphDeploymentID, String>::from_iter(
                current_deployments
                    .into_iter()
                    .map(|(subgraph, deployment)| (deployment, subgraph)),
            );
            tracing::info!(current_deployments = %deployment_to_subgraph.len(), indexed_deployments = %indexer_statuses.len());
            for (deployment, indexer_statuses) in indexer_statuses.iter() {
                let subgraph = match deployment_to_subgraph.get(&deployment) {
                    Some(subgraph) => subgraph.clone(),
                    None => continue,
                };
                // TODO: We are assuming the network is mainnet for now.
                let network = "mainnet".to_string();
                tracing::trace!(
                    %network,
                    %subgraph,
                    ?deployment,
                    indexer_statuses = indexer_statuses.len(),
                );
                let writer = deployments
                    .write(&QualifiedSubgraph { network, subgraph })
                    .await;
                writer.id.write(deployment.clone());
                writer.indexers.write(indexer_statuses.iter().map(|status| status.id).collect());
            }
            let statuses = HashMap::<Address, ParsedIndexerStatus>::from_iter(indexer_statuses.into_iter().flat_map(|(_, statuses)| statuses.into_iter().map(|status| (status.id, status))));
            tracing::info!(indexers = %statuses.len());
            for (indexer, status)  in statuses {
                let indexer = indexers.write(&indexer).await;
                indexer.stake.write(status.staked);
                indexer.delegated_stake.write(status.delegated);
            }
        }
    }.in_current_span());
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
