# Network Module

Network topology management for subgraph/deployment resolution.

## Module Overview

| File                     | Visibility | Purpose                                                 |
| ------------------------ | ---------- | ------------------------------------------------------- |
| `service.rs`             | pub        | `NetworkService` - main resolution interface            |
| `snapshot.rs`            | internal   | In-memory topology state (`NetworkTopologySnapshot`)    |
| `subgraph_client.rs`     | pub        | Fetches data from network subgraph via trusted indexers |
| `indexer_processing.rs`  | internal   | Processes indexer info using type-state pattern         |
| `pre_processing.rs`      | internal   | Validates and converts raw subgraph data                |
| `subgraph_processing.rs` | internal   | Processes subgraph/deployment info                      |
| `cost_model.rs`          | pub        | Resolves indexer query fees                             |
| `indexing_progress.rs`   | pub        | Resolves indexer block progress                         |
| `host_filter.rs`         | pub        | IP-based indexer filtering                              |
| `version_filter.rs`      | pub        | Version-based indexer filtering                         |
| `poi_filter.rs`          | pub        | Proof-of-indexing filtering                             |
| `indexer_blocklist.rs`   | pub        | Manual indexer blocklist management                     |

## Data Flow

```
SubgraphClient.fetch()
        |
        v
pre_processing::into_internal_*()
        |
        v
subgraph_processing::process_*()
        |
        v
indexer_processing::process_info()
        |
        v
snapshot::new_from()
        |
        v
NetworkTopologySnapshot (published via watch channel)
```

## Key Types

- `NetworkService` - Query resolution interface (cloneable handle)
- `ResolvedSubgraphInfo` - Result of subgraph/deployment resolution
- `Indexing` - Single indexer+deployment combination with resolved info
- `IndexingId` - Unique key (indexer, deployment) for indexings
- `IndexingInfo<P, C>` - Type-state pattern for indexing processing stages

## Update Cycle

Every 30 seconds:

1. Fetch subgraph info from network subgraph
2. Validate and pre-process data
3. Resolve indexer info (version, POI, progress, fees)
4. Build new `NetworkTopologySnapshot`
5. Publish via watch channel

## Filters

Indexers can be filtered out by:

- **HostFilter**: IP address blocklist
- **VersionFilter**: Minimum indexer-service and graph-node versions
- **PoiFilter**: Bad proof-of-indexing blocklist
- **IndexerBlocklist**: Manual per-indexer-deployment blocklist
