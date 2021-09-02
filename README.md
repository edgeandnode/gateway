# graph-gateway
Graph Gateway

## Observability

This application is instrumented using the Tokio [Tracing](https://github.com/tokio-rs/tracing) library.

### Logs

Logs filtering is set using the `RUST_LOG` environment variable. For example, if you would like to set the default log level to `trace`, but want to set the log level for the `graph_gateway::ws_client` module to `debug`, you would use the following:
```
RUST_LOG="trace,graph_gateway::ws_client=debug"
```
More details on evironment variable filtering: https://docs.rs/tracing-subscriber/0.2.20/tracing_subscriber/filter/struct.EnvFilter.html

### Metrics

TODO
