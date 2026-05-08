# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
cargo build
cargo build --release

# Run tests
cargo test

# Run a single test
cargo test <test_name>
cargo test <module>::<test_name>

# Check compilation without building
cargo check

# Run the gateway
cargo run --release -- path/to/config.json
```

**Dependencies:** Requires OpenSSL (`brew install openssl` on macOS) for rdkafka.

## Architecture Overview

This is The Graph Gateway - a Rust service that routes GraphQL queries from clients to indexers in The Graph Network. The gateway manages indexer discovery, selection, query routing, and payment (TAP receipts).

### Core Flow

1. **Client Request** → `src/client_query.rs` handles incoming GraphQL queries via axum routes
2. **Auth** → `src/auth.rs` validates API keys (from Studio API, Kafka, or fixed config)
3. **Subgraph Resolution** → `src/network/service.rs` (`NetworkService`) resolves subgraph/deployment IDs to available indexers
4. **Indexer Selection** → Uses `indexer-selection` crate with criteria: success rate, latency, chain sync status, stake, fees
5. **Query Execution** → `src/indexer_client.rs` sends requests to up to 3 indexers concurrently
6. **Payment** → `src/receipts.rs` creates TAP receipts for each indexer request
7. **Reporting** → `src/reports.rs` exports metrics to Kafka topics

### Key Modules

- **`src/network/`** - Network topology management
  - `service.rs` - `NetworkService` provides subgraph resolution
  - `snapshot.rs` - `NetworkTopologySnapshot` holds current indexer/deployment state
  - `subgraph_client.rs` - Queries network subgraph via trusted indexers
  - `indexer_processing.rs` - Processes indexer info (versions, POIs, costs)
  - `poi_filter.rs` - Filters indexers by blocked POIs
  - `version_filter.rs` - Enforces minimum indexer versions

- **`src/client_query.rs`** - Main query handling logic
  - `handle_query` - Standard query endpoint
  - `handle_indexer_query` - Direct indexer query endpoint
  - `build_candidates_list` - Prepares indexer candidates for selection

- **`src/block_constraints.rs`** - Parses GraphQL for block requirements, rewrites queries

- **`src/budgets.rs`** - Controls query fee targeting

- **`src/chain.rs` / `src/chains.rs`** - Chain head tracking

### Configuration

Configuration is JSON-based, loaded from a file path passed as the first CLI argument. Structure defined in `src/config.rs` (`Config` struct). Key settings:
- `api_keys` - API key source (Studio endpoint, Kafka topic, or fixed list)
- `trusted_indexers` - Bootstrap indexers for network subgraph
- `receipts` - TAP signer configuration (payer, chain_id, verifier addresses)
- `min_indexer_version` / `min_graph_node_version` - Version requirements

### API Endpoints

- `POST /api/subgraphs/id/{subgraph_id}` - Query by subgraph ID
- `POST /api/deployments/id/{deployment_id}` - Query by deployment ID
- `POST /api/deployments/id/{deployment_id}/indexers/id/{indexer}` - Direct indexer query
- Legacy: `POST /api/{api_key}/subgraphs/id/{subgraph_id}` (API key in path)

### Error Types

Errors defined in `src/errors.rs`:
- `Error::Auth` - Authentication/authorization failures
- `Error::SubgraphNotFound` - Invalid subgraph/deployment
- `Error::BadQuery` - Invalid GraphQL
- `Error::NoIndexers` - No indexers available
- `Error::BadIndexers` - All indexers failed

### Logging

Use `RUST_LOG` env var for filtering: `RUST_LOG="info,graph_gateway=debug"`

Log levels follow standard conventions - `error` for impacting issues, `warn` for recoverable issues, `info` for production tracing, `debug`/`trace` for development.
