use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

pub fn init_tracing(json: bool) {
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::try_new("info,graph_gateway=debug").unwrap()
    });
    let defaults = tracing_subscriber::registry().with(filter_layer);
    let fmt_layer = tracing_subscriber::fmt::layer();
    if json {
        defaults
            .with(fmt_layer.json().with_current_span(false))
            .init();
    } else {
        defaults.with(fmt_layer).init();
    }
}
