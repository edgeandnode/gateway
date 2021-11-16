use crate::{
    indexer_selection::{
        self, CostModelSource, IndexerDataReader, IndexerDataWriter, Indexing, IndexingData,
        IndexingStatus, SecretKey, SelectionFactors,
    },
    prelude::{shared_lookup::SharedLookupWriter, *},
    query_engine::{APIKey, InputWriters},
};
use eventuals::EventualExt as _;
use graphql_client::{GraphQLQuery, Response};
use im;
use lazy_static::lazy_static;
use prometheus;
use reqwest;
use serde_json::{json, Value as JSON};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::{sync::Mutex, time::sleep};
use tracing::{self, Instrument};
use uuid::Uuid;

pub fn create(
    network: String,
    agent_url: String,
    poll_interval: Duration,
    gateway_id: Uuid,
    signer_key: SecretKey,
    inputs: InputWriters,
    api_keys: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
) -> &'static Metrics {
    let _trace = tracing::info_span!("sync client", ?poll_interval).entered();
    let InputWriters {
        indexer_inputs:
            indexer_selection::InputWriters {
                slashing_percentage,
                usd_to_grt_conversion,
                indexers,
                indexings,
            },
        current_deployments,
        deployment_indexers,
        indexers: indexer_selection,
    } = inputs;
    let indexings = Arc::new(Mutex::new(indexings));

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
    create_sync_client::<CurrentDeployments, _>(
        gateway_id,
        agent_url.clone(),
        poll_interval,
        current_deployments::OPERATION_NAME,
        current_deployments::QUERY,
        parse_current_deployments,
        current_deployments,
    );
    handle_indexers(
        network,
        indexers,
        deployment_indexers,
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
        indexings.clone(),
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
        indexings.clone(),
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
        indexings.clone(),
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
    &METRICS
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
                let result = execute_query::<Q, T>(
                    gateway_id,
                    &agent_url,
                    operation,
                    query,
                    parse_data,
                    &client,
                    &mut last_update_id,
                )
                .in_current_span()
                .await;
                match result {
                    Some(data) => {
                        with_metric(&METRICS.queries_ok, &[operation], |c| c.inc());
                        writer.write(data);
                    }
                    None => {
                        with_metric(&METRICS.queries_failed, &[operation], |c| c.inc());
                    }
                };
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
                        id: value.api_key.id,
                        key: value.api_key.key,
                        user_id: value.user.id,
                        user_address: value.user.eth_address.parse().ok()?,
                        queries_activated: value.user.queries_activated,
                        subgraphs: value
                            .subgraphs
                            .into_iter()
                            .map(|subgraph| (subgraph.network_id, subgraph.id as i32))
                            .collect(),
                        deployments: value
                            .deployments
                            .into_iter()
                            .filter_map(|id| SubgraphDeploymentID::from_ipfs_hash(&id))
                            .collect(),
                        domains: value
                            .domains
                            .into_iter()
                            .map(|domain| (domain.name, domain.id as i32))
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
                        deployment: deployment?,
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
) -> Option<Ptr<HashMap<String, SubgraphDeploymentID>>> {
    use current_deployments::{CurrentDeploymentsData, ResponseData};
    let values = match data {
        ResponseData {
            data: Some(CurrentDeploymentsData { value, .. }),
        } => value,
        _ => return None,
    };
    let parsed = values.into_iter().filter_map(|value| {
        Some((
            value.subgraph,
            SubgraphDeploymentID::from_ipfs_hash(&value.deployment)?,
        ))
    });
    Some(Ptr::new(HashMap::from_iter(parsed)))
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
            let deployment = SubgraphDeploymentID::from_ipfs_hash(&value.deployment);
            value.statuses.into_iter().filter_map(move |status| {
                Some(ParsedIndexingStatus {
                    indexing: Indexing {
                        deployment: deployment?,
                        indexer: status.indexer.id.parse().ok()?,
                    },
                    network: status.network,
                    block_number: status.block.map(|b| b.number as u64),
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
                    deployment: SubgraphDeploymentID::from_ipfs_hash(&value.deployment)?,
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
                    deployment: SubgraphDeploymentID::from_ipfs_hash(
                        &value.subgraph_deployment_id,
                    )?,
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

fn handle_indexers(
    network: String,
    indexers: SharedLookupWriter<Address, IndexerDataReader, IndexerDataWriter>,
    mut deployment_indexers: EventualWriter<
        Ptr<HashMap<SubgraphDeploymentID, im::Vector<Address>>>,
    >,
    indexer_statuses: Eventual<Ptr<Vec<(SubgraphDeploymentID, Vec<ParsedIndexerInfo>)>>>,
) {
    let indexers = Arc::new(Mutex::new(indexers));
    indexer_statuses
        .pipe_async(move |indexer_statuses| {
            let _span = tracing::info_span!("handle_indexers").entered();
            tracing::info!(indexed_deployments = %indexer_statuses.len());
            deployment_indexers.write(Ptr::new(
                indexer_statuses
                    .iter()
                    .map(|(deployment, indexer_statuses)| {
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
    indexings: Arc<Mutex<SharedLookupWriter<Indexing, SelectionFactors, IndexingData>>>,
    indexing_statuses: Eventual<Ptr<Vec<ParsedIndexingStatus>>>,
) {
    indexing_statuses
        .pipe_async(move |indexing_statuses| {
            let indexer_selection = indexer_selection.clone();
            let indexings = indexings.clone();
            async move {
                tracing::info!(indexing_statuses = %indexing_statuses.len());
                let mut latest_blocks = HashMap::<String, u64>::new();
                let mut indexings = indexings.lock().await;
                for status in indexing_statuses.iter() {
                    let latest = match latest_blocks.entry(status.network.clone()) {
                        Entry::Occupied(entry) => *entry.get(),
                        Entry::Vacant(entry) => *entry.insert(
                            indexer_selection
                                .latest_block(&status.network)
                                .await
                                .map(|block| block.number)
                                .unwrap_or(0),
                        ),
                    };
                    indexings
                        .write(&status.indexing)
                        .await
                        .status
                        .write(IndexingStatus {
                            block: status.block_number.unwrap_or(0),
                            latest,
                        });
                }
            }
            .instrument(tracing::info_span!("handle_indexing_statuses"))
        })
        .forever();
}

fn handle_transfers(
    indexings: Arc<Mutex<SharedLookupWriter<Indexing, SelectionFactors, IndexingData>>>,
    transfers: Eventual<Ptr<Vec<ParsedTransfer>>>,
) {
    let mut used_transfers = Ptr::<Vec<ParsedTransfer>>::default();
    transfers
        .pipe_async(move |transfers| {
            let used_transfers = std::mem::replace(&mut used_transfers, transfers.clone());
            let indexings = indexings.clone();
            async move {
                tracing::info!(transfers = %transfers.len());
                METRICS.transfers.set(transfers.len() as i64);
                // Add new transfers.
                let mut lock = indexings.lock().await;
                for transfer in transfers.iter() {
                    if used_transfers.iter().any(|t| t.id == transfer.id) {
                        continue;
                    }
                    lock.write(&transfer.indexing)
                        .await
                        .add_transfer(transfer.id, &transfer.collateral, transfer.signer_key)
                        .await;
                }
                // Remove old transfers.
                for transfer in used_transfers.iter() {
                    if transfers.iter().any(|t| t.id == transfer.id) {
                        continue;
                    }
                    lock.write(&transfer.indexing)
                        .await
                        .remove_transfer(&transfer.id)
                        .await;
                }
            }
            .instrument(tracing::info_span!("handle_transfers"))
        })
        .forever();
}

fn handle_allocations(
    indexings: Arc<Mutex<SharedLookupWriter<Indexing, SelectionFactors, IndexingData>>>,
    signer_key: SecretKey,
    allocations: Eventual<Ptr<Vec<ParsedAllocation>>>,
) {
    let mut used_allocations = Ptr::<Vec<ParsedAllocation>>::default();
    allocations
        .pipe_async(move |allocations| {
            let used_allocations = std::mem::replace(&mut used_allocations, allocations.clone());
            let indexings = indexings.clone();
            async move {
                tracing::info!(allocations = %allocations.len());
                METRICS.allocations.set(allocations.len() as i64);
                // Add new allocations.
                let mut lock = indexings.lock().await;
                for allocation in allocations.iter() {
                    if used_allocations.iter().any(|t| t.id == allocation.id) {
                        continue;
                    }
                    lock.write(&allocation.indexing)
                        .await
                        .add_allocation(allocation.id, signer_key)
                        .await;
                }
                // Remove old allocations.
                for allocation in used_allocations.iter() {
                    if allocations.iter().any(|t| t.id == allocation.id) {
                        continue;
                    }
                    lock.write(&allocation.indexing)
                        .await
                        .remove_allocation(&allocation.id)
                        .await;
                }
            }
            .instrument(tracing::info_span!("handle_allocations"))
        })
        .forever();
}

#[derive(Clone)]
pub struct Metrics {
    pub allocations: prometheus::IntGauge,
    pub transfers: prometheus::IntGauge,
    pub queries_ok: prometheus::IntCounterVec,
    pub queries_failed: prometheus::IntCounterVec,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics {
        allocations: prometheus::register_int_gauge!("allocations", "Total allocations").unwrap(),
        transfers: prometheus::register_int_gauge!("transfers", "Total transfers").unwrap(),
        queries_ok: prometheus::register_int_counter_vec!(
            "gateway_network_subgraph_client_successful_queries",
            "Successful network subgraph queries",
            &["tag"],
        )
        .unwrap(),
        queries_failed: prometheus::register_int_counter_vec!(
            "gateway_network_subgraph_client_failed_queries",
            "Failed network subgraph queries",
            &["tag"],
        )
        .unwrap(),
    };
}
