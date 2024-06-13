# Incident Response

## Notes

- This document assumes logs can be filtered using [Loki LogQL](https://grafana.com/docs/loki/latest/query/log_queries/). See [logs.md](./logs.md) for a list of the log fields available.

- This doc uses the [Graph Network Arbitrum subgraph](https://thegraph.com/explorer/subgraphs/DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp?view=About&chain=arbitrum-one) for examples:
  - `subgraph_id: DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp`
  - `deployment_id: QmSWxvd8SaQK6qZKJ7xtfxCCGoRzGnoi2WNzmJYYJW9BXY`

## Common Log Queries

- `|= "client_request" |= "result" != "Ok"`
- `|= "indexer_request" |= "result" != "Ok"`
- `|= "client_request" |= "indexer_errors" != "{}"`

## Scenarios

### No Indexer Available to Serve Client Query

- check indexer errors:

  ```ts
  |~ "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp|QmSWxvd8SaQK6qZKJ7xtfxCCGoRzGnoi2WNzmJYYJW9BXY"
  |= "client_request" |= "indexer_errors" != "{}"
  ```

- check for failed client queries:

  ```ts
  |~ "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp|QmSWxvd8SaQK6qZKJ7xtfxCCGoRzGnoi2WNzmJYYJW9BXY"
  |= "client_request" |= "result" != "Ok"
  ```

For indexers, note that automated allocation management might not allocate to a subgraph deployment if it doesn’t meet requirements like minimum signal.

### Bad/Inconsistent Query Responses

- Graphix is a useful tool to check if allocated indexers have divergent POIs, and might indicate which indexers are delivering the bad responses.

  If tools like Graphix are not available, you can query the relevant indexers manually to get their POIs:

  ```bash
  curl ${indexer_url}/status \
    -H 'content-type: application/json' \
    -d '{"query": "{ publicProofsOfIndexing(requests: [{deployment: \"${deployment}\" blockNumber: ${block_number}}]) { deployment proofOfIndexing block { number } } }"}'
  ```

- If a POI is identified that should be blocked, it should be added to the gateway config’s `poi_blocklist`.

- Does the query rely on a graph-node feature that is unsupported?

  This is an open problem, see issue #526.

- As a worst-case measure, the gateway config also includes an indexer blocklist `bad_indexers`. This should be used temporarily, and with caution.

### Degraded performance on multiple subgraphs

- `|= "ERROR"`: error logs may show negative impacts on the gateway's ability to serve queries or make payments.
  - `|= "poll_subgraphs_err"`: failures to poll asubgraph (potentially the network subgraph)

### Other

- For other scenarios, it may be useful to identify a query where some issue occurred. Then filter for all logs containing the corresponding `request_id`.
