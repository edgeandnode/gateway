use crate::{
    indexer_selection::IndexerPreferences,
    prelude::*,
    query_engine::{APIKey, VolumeEstimator},
};
use graphql_client::{GraphQLQuery, Response};
use lazy_static::lazy_static;
use reqwest;
use serde_json::{json, Value as JSON};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::Mutex, time::sleep};
use tracing::{self, Instrument};

pub fn create(
    agent_url: String,
    poll_interval: Duration,
    slashing_percentage: EventualWriter<PPM>,
    usd_to_grt_conversion: EventualWriter<USD>,
    api_keys: EventualWriter<Ptr<HashMap<String, Arc<APIKey>>>>,
    accept_empty: bool,
) {
    let _trace = tracing::info_span!("sync client", ?poll_interval).entered();

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
                        is_subsidized: value.api_key.is_subsidized,
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
            tracing::trace!(api_keys = %parsed.len());
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

fn parse_conversion_rates(data: conversion_rates::ResponseData) -> Option<GRT> {
    use conversion_rates::{ConversionRatesData, ConversionRatesDataValue};
    match data {
        conversion_rates::ResponseData {
            data:
                Some(ConversionRatesData {
                    value:
                        ConversionRatesDataValue {
                            dai_to_grt: Some(grt_per_usd),
                            ..
                        },
                    ..
                }),
        } => {
            tracing::debug!(?grt_per_usd);
            grt_per_usd.parse::<GRT>().ok()
        }
        _ => None,
    }
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

#[derive(Clone)]
pub struct Metrics {
    pub queries: ResponseMetricVecs,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics {
        queries: ResponseMetricVecs::new(
            "gateway_network_subgraph_client_queries",
            "network subgraph queries",
            &["tag"],
        ),
    };
}
