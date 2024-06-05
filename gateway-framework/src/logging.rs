use tracing_subscriber::{prelude::*, registry, EnvFilter};

pub fn init(executable_name: String, json: bool) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::try_new(format!("info,{executable_name}=debug")).unwrap());

    let log_default_layer = (!json).then(tracing_subscriber::fmt::layer);
    let log_json_layer = json.then(|| {
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
    });

    registry()
        .with(env_filter)
        .with(log_default_layer)
        .with(log_json_layer)
        .init();
}
