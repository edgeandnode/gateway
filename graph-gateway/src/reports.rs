use crate::{
    client_query,
    indexer_client::{IndexerError, ResponsePayload},
    protobuf::kafka::{GatewaySubscriptionQueryResult, StatusCode},
};
use prelude::{tracing::span, *};
use prost::Message;
use rdkafka::error::KafkaResult;
use serde::Deserialize;
use serde_json::{json, Map};
use std::error::Error;
use tracing_subscriber::{filter::FilterFn, layer, prelude::*, registry, EnvFilter, Layer};

// TODO: integrate Prometheus metrics

pub const CLIENT_QUERY_TARGET: &str = "client_query";
pub const INDEXER_QUERY_TARGET: &str = "indexer_query";

pub struct KafkaClient {
    producer: rdkafka::producer::ThreadedProducer<rdkafka::producer::DefaultProducerContext>,
}

impl KafkaClient {
    pub fn new(config: &rdkafka::ClientConfig) -> KafkaResult<KafkaClient> {
        let producer = config.create_with_context(rdkafka::producer::DefaultProducerContext)?;
        Ok(KafkaClient { producer })
    }

    pub fn send(&self, topic: &str, payload: &[u8]) {
        let record = rdkafka::producer::BaseRecord::<'_, (), [u8]>::to(topic).payload(payload);
        if let Err((kafka_producer_err, _)) = self.producer.send(record) {
            tracing::error!(%kafka_producer_err, %topic);
        }
    }
}

pub fn init(kafka: &'static KafkaClient, log_json: bool) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::try_new("info,graph_gateway=debug").unwrap());

    let log_default_layer = (!log_json).then(tracing_subscriber::fmt::layer);
    let log_json_layer = log_json.then(|| {
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
    });

    let kafka_layer = KafkaLayer(kafka).with_filter(FilterFn::new(|metadata| {
        (metadata.target() == CLIENT_QUERY_TARGET) || (metadata.target() == INDEXER_QUERY_TARGET)
    }));

    registry()
        .with(env_filter)
        .with(log_default_layer)
        .with(log_json_layer)
        .with(kafka_layer)
        .init();
}

struct KafkaLayer(&'static KafkaClient);

impl<S> Layer<S> for KafkaLayer
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        let mut fields = Map::<String, serde_json::Value>::new();
        attrs.record(&mut CollectFields(&mut fields));
        let span = ctx.span(id).unwrap();
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: layer::Context<'_, S>) {
        let span = ctx.span(span).unwrap();
        let mut extensions = span.extensions_mut();
        let fields = match extensions.get_mut() {
            Some(fields) => fields,
            None => {
                error_log(span.metadata().target(), "report already sent for span");
                return;
            }
        };
        values.record(&mut CollectFields(fields));
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: layer::Context<'_, S>) {
        let span = ctx.event_span(event).unwrap();
        let mut extensions = span.extensions_mut();
        let fields = match extensions.get_mut() {
            Some(fields) => fields,
            None => {
                error_log(span.metadata().target(), "report already sent for span");
                return;
            }
        };
        event.record(&mut CollectFields(fields));

        // TODO: ideally we would always send the message when span closes, but it seems we can't
        // access the stored fields in `on_close`.
        if !fields.contains_key("status_message") {
            return;
        }

        let mut fields: Map<String, serde_json::Value> = extensions.remove().unwrap();
        // insert values from parent spans
        for span in ctx.event_scope(event).unwrap().skip(1) {
            let extensions = span.extensions();
            if let Some(scope_fields) = extensions.get::<Map<String, serde_json::Value>>() {
                for (key, value) in scope_fields {
                    if !fields.contains_key(key) {
                        fields.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        match event.metadata().target() {
            CLIENT_QUERY_TARGET => report_client_query(self.0, fields),
            INDEXER_QUERY_TARGET => report_indexer_query(self.0, fields),
            _ => unreachable!("invalid event target for KafkaLayer"),
        }
    }
}

fn error_log(target: &str, message: &str) {
    let log = serde_json::to_string(&json!({
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        "level": "ERROR",
        "fields": { "message": message },
        "target": target,
    }))
    .unwrap();
    println!("{log}");
}

struct CollectFields<'a>(&'a mut Map<String, serde_json::Value>);
impl tracing_subscriber::field::Visit for CollectFields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), format!("{:?}", value).into());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_error(&mut self, field: &tracing::field::Field, value: &(dyn Error + 'static)) {
        self.0
            .insert(field.name().to_owned(), value.to_string().into());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        debug_assert!(false, "u128 should not be serialized to JSON");
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        debug_assert!(false, "1128 should not be serialized to JSON");
        self.0.insert(field.name().to_owned(), value.into());
    }
}

fn report_client_query(kafka: &KafkaClient, fields: Map<String, serde_json::Value>) {
    #[derive(Deserialize)]
    struct Fields {
        query_id: String,
        graph_env: String,
        status_message: String,
        status_code: i32,
        legacy_status_message: String,
        legacy_status_code: u32,
        start_time_ms: u64,
        deployment: Option<String>,
        api_key: Option<String>,
        ticket_name: Option<String>,
        ticket_user: Option<String>,
        ticket_signer: Option<String>,
        subgraph_chain: Option<String>,
        query_count: Option<u32>,
        budget_grt: Option<f32>,
        indexer_fees_grt: Option<f32>,
        query: Option<String>,
        variables: Option<String>,
    }
    let fields = match serde_json::from_value::<Fields>(fields.into()) {
        Ok(fields) => fields,
        Err(err) => {
            error_log(
                CLIENT_QUERY_TARGET,
                &format!("failed to report client query: {}", err),
            );
            return;
        }
    };

    let timestamp = unix_timestamp();
    let response_time_ms = timestamp.saturating_sub(fields.start_time_ms) as u32;

    // data science: TODO: what relies on this?
    let log = serde_json::to_string(&json!({
        "target": CLIENT_QUERY_TARGET,
        "level": "INFO",
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        "fields": {
            "message": "Client query",
            "query_id": &fields.query_id,
            "ray_id": &fields.query_id, // In production this will be the Ray ID.
            "query": fields.query.as_deref().unwrap_or(""),
            "variables": fields.variables.as_deref().unwrap_or(""),
        },
    }))
    .unwrap();
    println!("{log}");

    if let (Some(ticket_user), Some(ticket_signer)) = (fields.ticket_user, fields.ticket_signer) {
        let payload = GatewaySubscriptionQueryResult {
            query_id: fields.query_id.clone(),
            status_code: fields.status_code,
            status_message: fields.status_message.clone(),
            response_time_ms,
            ticket_user,
            ticket_signer,
            ticket_name: fields.ticket_name,
            deployment: fields.deployment.clone(),
            subgraph_chain: fields.subgraph_chain.clone(),
            query_count: fields.query_count,
            query_budget: fields.budget_grt,
            indexer_fees: fields.indexer_fees_grt,
        };
        kafka.send(
            "gateway_subscription_query_results",
            &payload.encode_to_vec(),
        );
    }

    // The following are maintained for backwards compatibility of existing data science
    // systems. They only apply to queries using Studio API keys.
    let api_key = match fields.api_key {
        Some(api_key) => api_key,
        None => return,
    };

    // data science: bigquery datasets still rely on this log line
    let log = serde_json::to_string(&json!({
        "target": CLIENT_QUERY_TARGET,
        "level": "INFO",
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        "fields": {
            "message": "Client query result",
            "query_id": &fields.query_id,
            "ray_id": &fields.query_id, // In production this will be the Ray ID.
            "deployment": fields.deployment.as_deref().unwrap_or(""),
            "network": fields.subgraph_chain.as_deref().unwrap_or(""),
            "api_key": &api_key,
            "query_count": fields.query_count.unwrap_or(0),
            "budget": fields.budget_grt.unwrap_or(0.0).to_string(),
            "fee": fields.indexer_fees_grt.unwrap_or(0.0),
            "response_time_ms": response_time_ms,
            "status": &fields.legacy_status_message,
            "status_code": fields.legacy_status_code,
        },
    }))
    .unwrap();
    println!("{log}");

    let kafka_msg = json!({
        "query_id": &fields.query_id,
        "ray_id": &fields.query_id, // In production this will be the Ray ID.
        "graph_env": &fields.graph_env,
        "timestamp": timestamp,
        "api_key": &api_key,
        "deployment": &fields.deployment.as_deref().unwrap_or(""),
        "network": &fields.subgraph_chain.as_deref().unwrap_or(""),
        "response_time_ms": response_time_ms,
        "budget": fields.budget_grt.unwrap_or(0.0).to_string(),
        "budget_float": fields.budget_grt,
        "query_count": fields.query_count.unwrap_or(0),
        "fee": fields.indexer_fees_grt.unwrap_or(0.0),
        "status": &fields.legacy_status_message,
        "status_code": fields.legacy_status_code,
    });
    kafka.send(
        "gateway_client_query_results",
        &serde_json::to_vec(&kafka_msg).unwrap(),
    );
}

fn report_indexer_query(kafka: &KafkaClient, fields: Map<String, serde_json::Value>) {
    #[derive(Deserialize)]
    struct Fields {
        query_id: String,
        graph_env: String,
        status_message: String,
        status_code: u32,
        response_time_ms: u32,
        deployment: String,
        subgraph_chain: String,
        indexer: String,
        url: String,
        blocks_behind: u64,
        fee_grt: f32,
        allocation: Option<String>,
        indexer_errors: Option<String>,
    }
    let fields = match serde_json::from_value::<Fields>(fields.into()) {
        Ok(fields) => fields,
        Err(err) => {
            error_log(
                INDEXER_QUERY_TARGET,
                &format!("failed to report indexer query: {}", err),
            );
            return;
        }
    };

    // data science: bigquery datasets still rely on this log line
    let log = serde_json::to_string(&json!({
        "target": INDEXER_QUERY_TARGET,
        "level": "INFO",
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        "fields": {
            "message": "Indexer attempt",
            "query_id": &fields.query_id,
            "ray_id": &fields.query_id, // In production this will be the Ray ID.
            "deployment": &fields.deployment,
            "indexer": &fields.indexer,
            "url": &fields.url,
            "blocks_behind": fields.blocks_behind,
            "attempt_index": 0,
            "api_key": "",
            "fee": fields.fee_grt,
            "response_time_ms": fields.response_time_ms,
            "allocation": &fields.allocation,
            "indexer_errors": &fields.indexer_errors,
            "status": &fields.status_message,
            "status_code": fields.status_code,
        },
    }))
    .unwrap();
    println!("{log}");

    let kafka_msg = json!({
        "query_id": &fields.query_id,
        "ray_id": &fields.query_id, // In production this will be the Ray ID.
        "graph_env": &fields.graph_env,
        "timestamp": unix_timestamp(),
        "api_key": "",
        "deployment": &fields.deployment,
        "network": &fields.subgraph_chain,
        "indexer": &fields.indexer,
        "url": &fields.url,
        "fee": fields.fee_grt,
        "utility": 1.0,
        "blocks_behind": fields.blocks_behind,
        "response_time_ms": fields.response_time_ms,
        "allocation": fields.allocation.as_deref().unwrap_or(""),
        "indexer_errors": fields.indexer_errors.as_deref().unwrap_or(""),
        "status": &fields.status_message,
        "status_code": fields.status_code,
    });
    kafka.send(
        "gateway_indexer_attempts",
        &serde_json::to_vec(&kafka_msg).unwrap(),
    );
}

pub fn status<T>(result: &Result<T, client_query::Error>) -> (String, i32) {
    match result {
        Ok(_) => ("200 OK".to_string(), StatusCode::Success.into()),
        Err(err) => match err {
            client_query::Error::Internal(_) => (err.to_string(), StatusCode::InternalError.into()),
            client_query::Error::InvalidAuth(_)
            | client_query::Error::InvalidDeploymentId(_)
            | client_query::Error::InvalidQuery(_)
            | client_query::Error::InvalidSubgraphId(_)
            | client_query::Error::SubgraphChainNotSupported(_) => {
                (err.to_string(), StatusCode::UserError.into())
            }
            client_query::Error::BlockNotFound(_)
            | client_query::Error::DeploymentNotFound(_)
            | client_query::Error::NoIndexers
            | client_query::Error::NoSuitableIndexer(_)
            | client_query::Error::SubgraphNotFound(_) => {
                (err.to_string(), StatusCode::NotFound.into())
            }
        },
    }
}

pub fn legacy_status<T>(result: &Result<T, client_query::Error>) -> (String, u32) {
    match result {
        Ok(_) => ("200 OK".to_string(), 0),
        Err(err) => match err {
            client_query::Error::BlockNotFound(_) => ("Unresolved block".to_string(), 604610595),
            client_query::Error::DeploymentNotFound(_) => (err.to_string(), 628859297),
            client_query::Error::Internal(_) => ("Internal error".to_string(), 816601499),
            client_query::Error::InvalidAuth(_) => ("Invalid API key".to_string(), 888904173),
            client_query::Error::InvalidDeploymentId(_) => (err.to_string(), 19391651),
            client_query::Error::InvalidQuery(_) => ("Invalid query".to_string(), 595700117),
            client_query::Error::InvalidSubgraphId(_) => (err.to_string(), 2992863035),
            client_query::Error::NoIndexers => (
                "No indexers found for subgraph deployment".to_string(),
                1621366907,
            ),
            client_query::Error::NoSuitableIndexer(_) => (
                "No suitable indexer found for subgraph deployment".to_string(),
                510359393,
            ),
            client_query::Error::SubgraphChainNotSupported(_) => (err.to_string(), 1760440045),
            client_query::Error::SubgraphNotFound(_) => (err.to_string(), 2599148187),
        },
    }
}

// 32-bit status, encoded as `| 31:28 prefix | 27:0 data |` (big-endian)
pub fn indexer_attempt_status_code(result: &Result<ResponsePayload, IndexerError>) -> u32 {
    let (prefix, data) = match &result {
        // prefix 0x0, followed by the HTTP status code
        Ok(_) => (0x0, 200_u32.to_be()),
        Err(IndexerError::NoAttestation) => (0x1, 0x0),
        Err(IndexerError::UnattestableError) => (0x2, 0x0),
        Err(IndexerError::Timeout) => (0x3, 0x0),
        Err(IndexerError::UnexpectedPayload) => (0x4, 0x0),
        Err(IndexerError::UnresolvedBlock) => (0x5, 0x0),
        Err(IndexerError::NoAllocation) => (0x7, 0x0),
        // prefix 0x6, followed by a 28-bit hash of the error message
        Err(IndexerError::Other(msg)) => (0x6, sip24_hash(&msg) as u32),
    };
    (prefix << 28) | (data & (u32::MAX >> 4))
}
