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



## Kafka dependencies
**If building on an Mac you will need to:
-----
**-brew install cyrus-sasl, openssl
-cmake
-gnu make


If building on an M1 
----
Have brew installed for arch 64.
Make sure there's no mangling of openssl in /usr/lib/openssl and /opt/homebrew/openssl/lib


you will need to:
export LDFLAGS="-L/opt/homebrew/opt/cyrus-sasl/lib -L/opt/homebrew/opt/openssl@3/lib"
export CPPFLAGS="-I/opt/homebrew/opt/cyrus-sasl/include -I/opt/homebrew/opt/openssl@3/include"
export PKG_CONFIG_PATH="/opt/homebrew/opt/cyrus-sasl/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig"

[Install librdkafka from source.](https://github.com/edenhill/librdkafka)
./configure --install-deps
make
sudo make install


You will also need to set the `rust-rdkafka` package feature to contain `dynamic-linking`. This will link to librdkafka compiled for the architecture instead of invoking cmake (has issues with linker when invoked w/ cargo).

rustup target add aarch64-apple-darwin 
cargo build 

RUST_LOG="librdkafka=trace,rdkafka::client=debug" cargo test  --package graph-gateway -- redpanda::tests::test_redpanda --exact --show-output --nocapture


Passing Kafka client through the requests:
  -Add into middleware?
  -Make available to the Query object