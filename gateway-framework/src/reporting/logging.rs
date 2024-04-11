use serde_json::json;
use tracing_subscriber::{prelude::*, registry, EnvFilter, Layer};

use super::kafka::{EventFilterFn, EventHandlerFn, KafkaClient, KafkaLayer};

pub struct LoggingOptions {
    pub executable_name: String,
    pub json: bool,
    pub event_filter: EventFilterFn,
    pub event_handler: EventHandlerFn,
}

pub fn init(kafka: &'static KafkaClient, options: LoggingOptions) {
    let LoggingOptions {
        executable_name,
        json,
        event_filter,
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
    .with_filter(event_filter);

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
