use crate::{
    block_resolver::BlockResolver,
    indexer_selection::{
        self, CostModelSource, IndexerDataReader, IndexerDataWriter, IndexerPreferences, Indexing,
        IndexingData, IndexingStatus, SecretKey, SelectionFactors,
    },
    prelude::{shared_lookup::SharedLookupWriter, *},
    query_engine::{APIKey, InputWriters, VolumeEstimator},
};
use eventuals::EventualExt as _;
use graphql_client::{GraphQLQuery, Response};
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

pub fn create(
    agent_url: String,
    poll_interval: Duration,
    signer_key: SecretKey,
    inputs: InputWriters,
    block_resolvers: Arc<HashMap<String, BlockResolver>>,
    api_keys: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
    accept_empty: bool,
) -> &'static Metrics {
    let _trace = tracing::info_span!("sync client", ?poll_interval).entered();
    let InputWriters {
        indexer_inputs:
            indexer_selection::InputWriters {
                slashing_percentage,
                usd_to_grt_conversion,
                indexers,
                indexings,
                ..
            },
        current_deployments,
        deployment_indexers,
        ..
    } = inputs;
    let indexings = Arc::new(Mutex::new(indexings));

    let mut api_key_usage = VolumeEstimations::new();
    create_sync_client::<APIKeys, _, _>(
        agent_url.clone(),
        poll_interval,
        api_keys::OPERATION_NAME,
        api_keys::QUERY,
        move |v| parse_api_keys(v, &mut api_key_usage),
        api_keys,
        accept_empty,
    );
    create_sync_client::<ConversionRates, _, _>(
        agent_url.clone(),
        poll_interval,
        conversion_rates::OPERATION_NAME,
        conversion_rates::QUERY,
        parse_conversion_rates,
        usd_to_grt_conversion,
        accept_empty,
    );
    create_sync_client::<NetworkParameters, _, _>(
        agent_url.clone(),
        poll_interval,
        network_parameters::OPERATION_NAME,
        network_parameters::QUERY,
        parse_network_parameters,
        slashing_percentage,
        accept_empty,
    );
    handle_cost_models(
        indexings.clone(),
        create_sync_client_input::<CostModels, _>(
            agent_url.clone(),
            poll_interval,
            cost_models::OPERATION_NAME,
            cost_models::QUERY,
            parse_cost_models,
            accept_empty,
        ),
    );
    create_sync_client::<CurrentDeployments, _, _>(
        agent_url.clone(),
        poll_interval,
        current_deployments::OPERATION_NAME,
        current_deployments::QUERY,
        parse_current_deployments,
        current_deployments,
        accept_empty,
    );
    handle_indexers(
        indexers,
        deployment_indexers,
        create_sync_client_input::<Indexers, _>(
            agent_url.clone(),
            poll_interval,
            indexers::OPERATION_NAME,
            indexers::QUERY,
            parse_indexers,
            accept_empty,
        ),
    );
    handle_indexing_statuses(
        block_resolvers,
        indexings.clone(),
        create_sync_client_input::<IndexingStatuses, _>(
            agent_url.clone(),
            poll_interval,
            indexing_statuses::OPERATION_NAME,
            indexing_statuses::QUERY,
            parse_indexing_statuses,
            accept_empty,
        ),
    );
    handle_allocations(
        indexings.clone(),
        signer_key,
        create_sync_client_input::<UsableAllocations, _>(
            agent_url.clone(),
            poll_interval,
            usable_allocations::OPERATION_NAME,
            usable_allocations::QUERY,
            parse_usable_allocations,
            accept_empty,
        ),
    );
    &METRICS
}

fn create_sync_client_input<Q, T>(
    agent_url: String,
    poll_interval: Duration,
    operation: &'static str,
    query: &'static str,
    parse_data: fn(Q::ResponseData) -> Option<T>,
    accept_empty: bool,
) -> Eventual<T>
where
    T: 'static + Clone + Eq + Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static,
{
    let (writer, reader) = Eventual::new();
    create_sync_client::<Q, T, _>(
        agent_url,
        poll_interval,
        operation,
        query,
        parse_data,
        writer,
        accept_empty,
    );
    reader
}

fn create_sync_client<Q, T, F>(
    agent_url: String,
    poll_interval: Duration,
    operation: &'static str,
    query: &'static str,
    mut parse_data: F,
    mut writer: EventualWriter<T>,
    accept_empty: bool,
) where
    F: 'static + FnMut(Q::ResponseData) -> Option<T> + Send,
    T: 'static + Clone + Eq + Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static,
{
    tokio::spawn(
        async move {
            let client = reqwest::Client::new();
            let mut last_update_id = "<none>".to_string();
            loop {
                let _timer =
                    with_metric(&METRICS.queries.duration, &[operation], |h| h.start_timer());
                let result = execute_query::<Q, T, F>(
                    &agent_url,
                    operation,
                    query,
                    &mut parse_data,
                    &client,
                    &mut last_update_id,
                    accept_empty,
                )
                .in_current_span()
                .await;
                match result {
                    Some(data) => {
                        with_metric(&METRICS.queries.ok, &[operation], |c| c.inc());
                        writer.write(data);
                    }
                    None => {
                        with_metric(&METRICS.queries.failed, &[operation], |c| c.inc());
                    }
                };
                sleep(poll_interval).await;
            }
        }
        .instrument(tracing::info_span!("poller", query = operation)),
    );
}

async fn execute_query<'f, Q, T, F>(
    agent_url: &'f str,
    operation: &'static str,
    query: &'static str,
    parse_data: &'f mut F,
    client: &'f reqwest::Client,
    last_update_id: &'f mut String,
    accept_empty: bool,
) -> Option<T>
where
    F: 'static + FnMut(Q::ResponseData) -> Option<T>,
    T: 'static + Clone + Eq + Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static,
{
    // TODO: Don't use graphql_client and and just rely on graphql-parser. This is a bit more trouble than it's worth.
    let body = json!({
        "operationName": operation,
        "query": query,
        "variables": {"lastUpdateId": last_update_id },
    });
    tracing::trace!(%operation, %last_update_id);
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
    if !accept_empty
        && response_data
            .as_ref()
            .and_then(|data| data.get("value"))
            .and_then(|value| Some(value.as_array()?.is_empty()))
            .unwrap_or(false)
    {
        tracing::warn!("ignoring empty value");
        return None;
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

struct VolumeEstimations {
    by_api_key: HashMap<String, Arc<Mutex<VolumeEstimator>>>,
    decay_list: Arc<parking_lot::Mutex<Option<Vec<Arc<Mutex<VolumeEstimator>>>>>>,
}

impl Drop for VolumeEstimations {
    fn drop(&mut self) {
        *self.decay_list.lock() = None;
    }
}

impl VolumeEstimations {
    pub fn new() -> Self {
        let decay_list = Arc::new(parking_lot::Mutex::new(Some(Vec::new())));
        let result = Self {
            by_api_key: HashMap::new(),
            decay_list: decay_list.clone(),
        };

        // Every 2 minutes, call decay on every VolumeEstimator in our collection.
        // This task will finish when the VolumeEstimations is dropped, because
        // drop sets the decay_list to None which breaks the loop.
        tokio::spawn(async move {
            loop {
                let start = Instant::now();
                let len = if let Some(decay_list) = decay_list.lock().as_deref() {
                    decay_list.len()
                } else {
                    return;
                };
                for i in 0..len {
                    let item = if let Some(decay_list) = decay_list.lock().as_deref() {
                        decay_list[i].clone()
                    } else {
                        return;
                    };
                    item.lock().await.decay();
                }
                let next = start + Duration::from_secs(120);
                tokio::time::sleep_until(next).await;
            }
        });
        result
    }
    pub fn get(&mut self, key: &str) -> Arc<Mutex<VolumeEstimator>> {
        match self.by_api_key.get(key) {
            Some(exist) => exist.clone(),
            None => {
                let result = Arc::new(Mutex::new(VolumeEstimator::default()));
                self.by_api_key.insert(key.to_owned(), result.clone());
                self.decay_list
                    .lock()
                    .as_mut()
                    .unwrap()
                    .push(result.clone());
                result
            }
        }
    }
}

fn parse_api_keys(
    data: api_keys::ResponseData,
    usage: &mut VolumeEstimations,
) -> Option<Ptr<HashMap<String, Arc<APIKey>>>> {
    match data {
        api_keys::ResponseData {
            data: Some(api_keys::ApiKeysData { value, .. }),
        } => {
            let parsed = value
                .into_iter()
                .filter_map(|value| {
                    let mut indexer_preferences = IndexerPreferences::default();
                    for preference in value.indexer_preferences {
                        match preference.name.as_ref() {
                            "Fastest speed" => indexer_preferences.performance = preference.weight,
                            "Lowest price" => {
                                indexer_preferences.price_efficiency = preference.weight
                            }
                            "Data freshness" => {
                                indexer_preferences.data_freshness = preference.weight
                            }
                            "Economic security" => {
                                indexer_preferences.economic_security = preference.weight
                            }
                            unexpected_indexer_preference_name => {
                                tracing::warn!(%unexpected_indexer_preference_name)
                            }
                        }
                    }
                    let max_budget = match value.api_key.query_budget_selection_type.as_str() {
                        "USER_SET" => value
                            .api_key
                            .query_budget_usd
                            .parse::<f64>()
                            .ok()
                            .and_then(|b| USD::try_from(b).ok()),
                        "DYNAMIC" | _ => None,
                    };
                    Some(APIKey {
                        usage: usage.get(&value.api_key.key),
                        id: value.api_key.id,
                        key: value.api_key.key,
                        user_id: value.user.id,
                        user_address: value.user.eth_address.parse().ok()?,
                        queries_activated: value.user.queries_activated,
                        max_budget,
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
                        indexer_preferences,
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
) -> Option<Ptr<HashMap<SubgraphID, SubgraphDeploymentID>>> {
    use current_deployments::{CurrentDeploymentsData, ResponseData};
    let values = match data {
        ResponseData {
            data: Some(CurrentDeploymentsData { value, .. }),
        } => value,
        _ => return None,
    };
    let parsed = values.into_iter().filter_map(|value| {
        Some((
            value.subgraph.parse::<SubgraphID>().ok()?,
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
    query_path = "graphql/usable_allocations.gql",
    response_derives = "Debug"
)]
struct UsableAllocations;

#[derive(Clone, Eq, PartialEq)]
struct ParsedAllocation {
    id: Address,
    indexing: Indexing,
    size: GRT,
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
                size: value.allocated_tokens.parse().ok()?,
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
    indexers: SharedLookupWriter<Address, IndexerDataReader, IndexerDataWriter>,
    deployment_indexers: EventualWriter<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>>,
    indexer_info: Eventual<Ptr<Vec<(SubgraphDeploymentID, Vec<ParsedIndexerInfo>)>>>,
) {
    let indexers = Arc::new(Mutex::new(indexers));
    let deployment_indexers = Arc::new(Mutex::new(deployment_indexers));
    let indexings = Arc::new(Mutex::new(HashMap::<Indexing, Instant>::new()));
    indexer_info
        .pipe_async(move |indexer_info| {
            let indexers = indexers.clone();
            let deployment_indexers = deployment_indexers.clone();
            let indexings = indexings.clone();
            async move {
                tracing::info!(indexed_deployments = %indexer_info.len());
                {
                    let mut indexings = indexings.lock().await;
                    let now = Instant::now();
                    for indexing in indexer_info.iter().flat_map(|(deployment, indexers)| {
                        indexers.iter().map(|info| Indexing {
                            deployment: deployment.clone(),
                            indexer: info.id,
                        })
                    }) {
                        indexings.insert(indexing, now);
                    }
                    let removal_delay = Duration::from_secs(90);
                    indexings
                        .retain(|_, last_update| now.duration_since(*last_update) < removal_delay);

                    let mut update = HashMap::<SubgraphDeploymentID, Vec<Address>>::new();
                    for indexing in indexings.iter().map(|(indexing, _)| indexing) {
                        match update.entry(indexing.deployment.clone()) {
                            Entry::Occupied(mut entry) => entry.get_mut().push(indexing.indexer),
                            Entry::Vacant(entry) => {
                                entry.insert(vec![indexing.indexer]);
                            }
                        };
                    }
                    deployment_indexers.lock().await.write(Ptr::new(update));
                }
                let statuses =
                    HashMap::<Address, ParsedIndexerInfo>::from_iter(indexer_info.iter().flat_map(
                        |(_, statuses)| statuses.iter().cloned().map(|status| (status.id, status)),
                    ));
                tracing::info!(indexers = %statuses.len());
                let mut indexers = indexers.lock().await;
                for (indexer, status) in statuses {
                    let indexer = indexers.write(&indexer).await;
                    indexer.url.write(Arc::new(status.url));
                    indexer.stake.write(status.staked);
                }
            }
            .instrument(tracing::info_span!("handle_indexers"))
        })
        .forever();
}

fn handle_indexing_statuses(
    block_resolvers: Arc<HashMap<String, BlockResolver>>,
    indexings: Arc<Mutex<SharedLookupWriter<Indexing, SelectionFactors, IndexingData>>>,
    indexing_statuses: Eventual<Ptr<Vec<ParsedIndexingStatus>>>,
) {
    indexing_statuses
        .pipe_async(move |indexing_statuses| {
            let block_resolvers = block_resolvers.clone();
            let indexings = indexings.clone();
            async move {
                tracing::info!(indexing_statuses = %indexing_statuses.len());
                let mut latest_blocks = HashMap::<String, u64>::new();
                let mut indexings = indexings.lock().await;
                for status in indexing_statuses.iter() {
                    let latest = match latest_blocks.entry(status.network.clone()) {
                        Entry::Occupied(entry) => *entry.get(),
                        Entry::Vacant(entry) => *entry.insert(
                            block_resolvers
                                .get(&status.network)
                                .and_then(|resolver| resolver.latest_block().map(|b| b.number))
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
                    let writer = lock.write(&allocation.indexing).await;
                    writer
                        .add_allocation(allocation.id, signer_key, allocation.size)
                        .await;
                    let total_allocation = writer.total_allocation().await;
                    drop(writer);
                    with_metric(
                        &METRICS.total_allocation,
                        &[
                            &allocation.indexing.deployment.to_string(),
                            &allocation.indexing.indexer.to_string(),
                        ],
                        |g| g.set(total_allocation.as_f64()),
                    );
                }
                // Remove old allocations.
                for allocation in used_allocations.iter() {
                    if allocations.iter().any(|t| t.id == allocation.id) {
                        continue;
                    }
                    let writer = lock.write(&allocation.indexing).await;
                    writer
                        .remove_allocation(&allocation.id, allocation.size)
                        .await;
                    let total_allocation = writer.total_allocation().await;
                    drop(writer);
                    if total_allocation == GRT::zero() {
                        lock.remove(&allocation.indexing).await;
                    }
                    with_metric(
                        &METRICS.total_allocation,
                        &[
                            &allocation.indexing.deployment.to_string(),
                            &allocation.indexing.indexer.to_string(),
                        ],
                        |g| g.set(total_allocation.as_f64()),
                    );
                }
            }
            .instrument(tracing::info_span!("handle_allocations"))
        })
        .forever();
}

#[derive(Clone)]
pub struct Metrics {
    pub allocations: prometheus::IntGauge,
    pub queries: ResponseMetricVecs,
    pub total_allocation: prometheus::GaugeVec,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics {
        allocations: prometheus::register_int_gauge!("allocations", "Total allocations").unwrap(),
        queries: ResponseMetricVecs::new(
            "gateway_network_subgraph_client_queries",
            "network subgraph queries",
            &["tag"],
        ),
        total_allocation: prometheus::register_gauge_vec!(
            "total_allocation",
            "Total total_allocation",
            &["deployment", "indexer"],
        )
        .unwrap(),
    };
}
