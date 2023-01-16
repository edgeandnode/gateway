# graph-gateway

Graph Gateway

## Overview

At a high level the gateway does 2 things:

1. Route client queries to indexers
2. Facilitate payments to indexers

### Query Lifecycle

1. The client query arrives in the form of a GraphQL request. The request path indicates the API key and the subgraph being queried.
2. If the `SubgraphID` (the GNS ID) is used, the subgraph deployment is resolved to the latest version for the subgraph.
3. The query budget is determined based on an estimation of the 30 day query volume for the API key.
4. A subset of up to 3 indexers are selected out of the set of available indexers for the subgraph deployment.
5. The query is made deterministic by replacing block numbers with hashes.
6. The query is forwarded to each selected indexer. See Indexer Selection section below.
7. Each indexer’s response, latency, etc. is fed back into the Indexer Selection Algorithm (ISA).
8. The first valid indexer response is returned it to the client. If no indexers return a valid response goto step 3.

### Payment Flow

- The current system for collecting funds from users is completely detached from the system for paying indexers
- For each query:
  1. A client makes a query to the Gateway, using some API key created in the Subgraph Studio
  2. The gateway forwards a `(Query, Receipt)` to each selected indexer. The receipt is a `(ReceiptID, GRT, Allocation, Signature)`. (`GRT` is a running total for the `ReceiptID`)
     - Side note: Receipts cannot be “voided” in Scalar. Conditional payments aren’t really possible. The router can attempt to reuse receipts to effectively overwrite previous outcomes and it is up to the indexer to check this and decide if it is willing to respond to queries from the router at all.
  3. A successful indexer response is returned to the client, and the API key owner’s debt is recorded by the gateway as `(APIKey, Instant, GRT)` to some billing database. The fee recorded is the sum of the fees for each selected indexer.
- Indexers pool the receipts for each allocation and submit them to the Gateway in exchange for a Voucher: `(AllocationID, GRT, Signature)`. This voucher may be redeemed by calling the AllocationExchange contract.
- It is assumed that the AllocationExchange contract has enough funds to redeem all vouchers signed by the Gateway.
- It is also assumed that batch jobs pull funds from API key owners based on the API key usage data from the billing database.
- The protocol currently hardcodes the AllocationExchange contract address
  - Effectively, this would be permissionless, once we get rid of [this line](https://github.com/graphprotocol/contracts/blob/468786de72f97e8ca6130132b6fc9238914f0260/contracts/staking/Staking.sol#L970)

## Observability

This application is instrumented using the Tokio [Tracing](https://github.com/tokio-rs/tracing) library.

### Logs

Logs filtering is set using the `RUST_LOG` environment variable. For example, if you would like to set the default log level to `trace`, but want to set the log level for the `indexer_selection::actor` module to `debug`, you would use the following:

```
RUST_LOG="trace,indexer_selection::actor=debug"
```

More details on evironment variable filtering: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html

### Metrics

Prometheus metrics are served at `:${METRICS_PORT}/metrics`

## Building the Docker Image

```bash
docker build . \
  --build-arg "GH_USER=${GH_USER}" \
  --build-arg "GH_TOKEN=${GH_TOKEN}" \
  -t edgeandnode/graph-gateway:latest
```
