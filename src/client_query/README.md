# Client Query Module

Client GraphQL query handling and indexer selection.

## Module Overview

| File                | Purpose                                                |
| ------------------- | ------------------------------------------------------ |
| `mod.rs`            | Query handlers, indexer selection, response processing |
| `context.rs`        | `Context` struct - shared services for handlers        |
| `query_selector.rs` | Axum extractor for subgraph/deployment IDs             |

## Query Endpoints

| Path                                             | Handler                | Description            |
| ------------------------------------------------ | ---------------------- | ---------------------- |
| `/api/subgraphs/id/{id}`                         | `handle_query`         | Query by subgraph ID   |
| `/api/deployments/id/{id}`                       | `handle_query`         | Query by deployment ID |
| `/api/deployments/id/{id}/indexers/id/{indexer}` | `handle_indexer_query` | Query specific indexer |

Legacy paths (with API key in URL) are also supported via middleware.

## Query Flow

```
1. Authorization
   - Validate API key
   - Check subgraph permissions

2. Resolution
   - NetworkService.resolve_with_*()
   - Get available indexers

3. Block Requirements
   - Parse query for block constraints
   - Filter indexers by block availability

4. Candidate Selection
   - Score by: success_rate, latency, fee, stake
   - Select top 3 candidates

5. Query Execution
   - Send to up to 3 indexers in parallel
   - Return first successful response

6. Response Handling
   - Strip _gateway_probe_ field
   - Report to Kafka
   - Update performance tracking
```

## Indexer Selection Algorithm

See `build_candidates_list` function:

1. Choose deployment version where indexers are within 30 blocks of chain head
2. Filter by block requirements (exact number, hash, or number_gte)
3. Filter by freshness (exclude indexers >30min behind for "latest" queries)
4. Score and select via `indexer_selection` crate

## Context Struct

The `Context` struct holds all services needed for query processing:

| Field                | Type                         | Purpose                         |
| -------------------- | ---------------------------- | ------------------------------- |
| `indexer_client`     | `IndexerClient`              | HTTP client for indexer queries |
| `receipt_signer`     | `&'static ReceiptSigner`     | TAP receipt signing             |
| `budgeter`           | `&'static Budgeter`          | Fee budget management           |
| `grt_per_usd`        | `watch::Receiver<...>`       | Exchange rate                   |
| `chains`             | `&'static Chains`            | Chain head tracking             |
| `network`            | `NetworkService`             | Subgraph resolution             |
| `indexing_perf`      | `IndexingPerformance`        | Performance tracking            |
| `attestation_domain` | `&'static Eip712Domain`      | Attestation verification        |
| `reporter`           | `mpsc::UnboundedSender<...>` | Kafka reporting                 |
