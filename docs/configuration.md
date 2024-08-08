# Configuration

Nearly all configuration is done via a single JSON configuration file, the path of which must be given as the first argument to the graph-gateway executable. e.g. `graph-gateway path/to/config.json`. The structure of the configuration file is defined in [config.rs](../src/config.rs) (`graph_gateway::config::Config`).

Logs filtering is set using the `RUST_LOG` environment variable. For example, if you would like to set the default log level to `info`, but want to set the log level for the `graph_gateway` module to `debug`, you would use `RUST_LOG="info,graph_gateway=debug"`. More details on evironment variable filtering: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html.
