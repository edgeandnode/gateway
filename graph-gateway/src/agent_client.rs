use graphql_client::{GraphQLQuery, Response};
use indexer_selection::actor::Update;
use lazy_static::lazy_static;
use prelude::{buffer_queue::QueueWriter, *};
use reqwest;
use serde_json::{json, Value as JSON};
use tokio::time::sleep;
use tracing::{self, Instrument};

trait Writer<T> {
    fn write(&mut self, value: T);
}

impl<T: eventuals::Value> Writer<T> for EventualWriter<T> {
    fn write(&mut self, value: T) {
        EventualWriter::write(self, value)
    }
}

impl<T> Writer<T> for QueueWriter<T> {
    fn write(&mut self, value: T) {
        let _ = QueueWriter::write(self, value);
    }
}

pub fn create(
    agent_url: String,
    poll_interval: Duration,
    update_writer: QueueWriter<Update>,
    accept_empty: bool,
) {
    let _trace = tracing::info_span!("sync client", ?poll_interval).entered();

    create_sync_client::<ConversionRates, _, _>(
        agent_url.clone(),
        poll_interval,
        conversion_rates::OPERATION_NAME,
        conversion_rates::QUERY,
        parse_conversion_rates,
        update_writer,
        accept_empty,
    );
}

fn create_sync_client<Q, T, F>(
    agent_url: String,
    poll_interval: Duration,
    operation: &'static str,
    query: &'static str,
    mut parse_data: F,
    mut writer: impl Writer<T> + Send + 'static,
    accept_empty: bool,
) where
    F: 'static + FnMut(Q::ResponseData) -> Option<T> + Send,
    T: Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static + Send,
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
    T: Send,
    Q: GraphQLQuery,
    Q::ResponseData: 'static + Send,
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
    query_path = "graphql/conversion_rates.gql",
    response_derives = "Debug"
)]
struct ConversionRates;

fn parse_conversion_rates(data: conversion_rates::ResponseData) -> Option<Update> {
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
            tracing::info!(usd_to_grt = %grt_per_usd);
            grt_per_usd
                .parse::<GRT>()
                .ok()
                .map(Update::USDToGRTConversion)
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
