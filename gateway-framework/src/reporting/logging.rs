use serde_json::json;
use tracing_subscriber::{filter::FilterFn, prelude::*, registry, EnvFilter, Layer};

use super::kafka::{EventHandlerFn, KafkaClient, KafkaLayer};

pub const CLIENT_REQUEST_TARGET: &str = "client_request";
pub const INDEXER_REQUEST_TARGET: &str = "indexer_request";

pub struct LoggingOptions {
    pub executable_name: String,
    pub json: bool,
    pub event_handler: EventHandlerFn,
}

pub fn init(kafka: &'static KafkaClient, options: LoggingOptions) {
    let LoggingOptions {
        executable_name,
        json,
        event_handler,
    } = options;

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::try_new(format!("info,{executable_name}=debug")).unwrap());

    let log_default_layer = (!json).then(tracing_subscriber::fmt::layer);
    let log_json_layer = json.then(|| {
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
    });

    let kafka_layer = KafkaLayer {
        client: kafka,
        event_handler,
    }
    .with_filter(FilterFn::new(|metadata| {
        metadata.target() == CLIENT_REQUEST_TARGET || metadata.target() == INDEXER_REQUEST_TARGET
    }));

    registry()
        .with(env_filter)
        .with(log_default_layer)
        .with(log_json_layer)
        .with(kafka_layer)
        .init();
}

pub fn error_log(target: &str, message: &str) {
    let log = serde_json::to_string(&json!({
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        "level": "ERROR",
        "fields": { "message": message },
        "target": target,
    }))
    .unwrap();
    println!("{log}");
}
