# graph-gateway

Graph Gateway

## Observability

This application is instrumented using the Tokio [Tracing](https://github.com/tokio-rs/tracing) library.

### Logs

Logs filtering is set using the `RUST_LOG` environment variable. For example, if you would like to set the default log level to `trace`, but want to set the log level for the `indexer_selection::actor` module to `debug`, you would use the following:

```
RUST_LOG="trace,indexer_selection::actor=debug"
```

More details on evironment variable filtering: https://docs.rs/tracing-subscriber/0.2.20/tracing_subscriber/filter/struct.EnvFilter.html

### Metrics

Prometheus metrics are served at `:${METRICS_PORT}/metrics`

## Building the Docker Image

```bash
docker build . \
  --build-arg "GH_USER=${GH_USER}" \
  --build-arg "GH_TOKEN=${GH_TOKEN}" \
  -t edgeandnode/graph-gateway:latest
```
