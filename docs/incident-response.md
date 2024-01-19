# Incident Response

## Notes

- This document assumes logs can be filtered using [Loki LogQL](https://grafana.com/docs/loki/latest/query/log_queries/). See [logs.md](./logs.md) for a list of the log fields available.

- This doc uses the [ENS Governance subgraph](https://thegraph.com/explorer/subgraphs/GyijYxW9yiSRcEd5u2gfquSvneQKi5QuvU3WZgFyfFSn?view=Overview&chain=arbitrum-one) for examples:
  - `subgraph_id: GyijYxW9yiSRcEd5u2gfquSvneQKi5QuvU3WZgFyfFSn`
  - `deployment_id: QmeBPZyEeaHyZAiFS2Q7cT3CESS49hhgGuT3E9S8RYoHNm`

## Scenarios

### No Indexer Available to Serve Client Query

- check failed queries:

  ```jsx
  |~ "GyijYxW9yiSRcEd5u2gfquSvneQKi5QuvU3WZgFyfFSn|QmeBPZyEeaHyZAiFS2Q7cT3CESS49hhgGuT3E9S8RYoHNm"
  |= "bad indexers"
  ```

For indexers, note that automated allocation management might not allocate to a subgraph deployment if it doesn’t meet requirements like minimum signal.

### Bad Client Query QoS

- check relevant [indexer errors](./errors.md):

  ```jsx
  |~ "GyijYxW9yiSRcEd5u2gfquSvneQKi5QuvU3WZgFyfFSn|QmeBPZyEeaHyZAiFS2Q7cT3CESS49hhgGuT3E9S8RYoHNm"
  |= "indexer_errors" != "indexer_query"
  | pattern "<timestamp> stdout F <json>" | line_format "{{.json}}" | json query_id="spans[0].query_id", errors="fields.indexer_errors" | line_format "{{.query_id}} {{.errors}}"
  ```

### Bad/Inconsistent Query Responses

- Graphix is a useful tool to checked if allocated indexers have divergent POIs, and might indicate give hints about which indexers might have the POI driving the bad responses.

- If a POI is identified that should be blocked, it should be added to the gateway config’s `poi_blocklist`.

- Does the query rely on a graph-node feature that is unsupported?

  This is an open problem, see issue #526.

- As a worst-case measure, the gateway config also includes an indexer blocklist `bad_indexers`. This should be used temporarily, and with caution.

### Degraded performance on multiple subgraphs

- `|= "ERROR"`: error logs may show negative impacts on the gateway's ability to serve queries or make payments.
  - `|= "fetch_block_err"`: failures to resolve block number/hash via RPC
  - `|= "poll_subgraphs_err"`: failures to poll asubgraph (potentially the network subgraph)

### Other

- For other scenarios, it may be useful to identify a query where some issue occurred. Then filter for all logs containing the corresponding `query_id`.

- common log queries

  ```jsx
  |~ "GyijYxW9yiSRcEd5u2gfquSvneQKi5QuvU3WZgFyfFSn|QmeBPZyEeaHyZAiFS2Q7cT3CESS49hhgGuT3E9S8RYoHNm"
  |= "status_message"
  ```
