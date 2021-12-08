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

Prometheus metrics are served at `:${METRICS_PORT}/metrics`

## Deployment

```bash
docker build . \
  --build-arg "GH_USER=${GH_USER}" \
  --build-arg "GH_TOKEN=${GH_TOKEN}" \
  -t edgeandnode/graph-gateway:latest
```

```bash
docker run --rm --name graph-gateway \
  -p "${PORT}:${PORT}" \
  -p "${METRICS_PORT}:${METRICS_PORT}" \
  -e "PORT=${PORT}" \
  -e "METRICS_PORT=${METRICS_PORT}" \
  -e "MNEMONIC=${MNEMONIC}" \
  -e "SYNC_AGENT=${SYNC_AGENT}" \
  -e "ETHEREUM_PROVIDERS=mainnet=eth-mainnet.alchemyapi.io/v2/${ALCHEMY_KEY},rinkeby=eth-rinkeby.alchemyapi.io/v2/${ALCHEMY_KEY}" \
  -e "NETWORK_SUBGRAPH=${NETWORK_SUBGRAPH}" \
  edgeandnode/graph-gateway
```

## Future performance considerations

These options should be considered once we can measure the "real world" performance impacts and determine if they are worth implementing.

- We will likely merge the gateway agent into network syncing.
- Replace the Rust default allocator with snmalloc via [snmalloc-rs](https://github.com/SchrodingerZhu/snmalloc-rs).
- Use an alternative method for quantizing indexer performance. Currently we use nearest triangle numbers, but I have found that something like the first 40 snmalloc sizeclasses works unreasonably well for our binning needs. These sizeclass calculations are are rescribed in section 2.6 of the snmalloc paper.
