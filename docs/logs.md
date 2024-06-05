# Logs

## Log Levels

- `error`: An unexpected state has been reached, which is likely to have a negative impact on the gateway's ability to serve queries or make payments.
- `warn`: An unexpected state has been reached, though it is recoverable and unlikely to have a negative impact on the gateway's ability to serve queries or make payments.
- `info`: Information that is commonly used to trace the execution of the major gateway subsystems in production.
- `debug`: Similar to `info`, but is often irrelevant when investigating gateway execution in production.
- `trace`: Information that is considered too verbose for production, but is often useful during development.

## client requests

Log events are emitted for each client request and all of its associated indexer requests. These can be found using the span label `client_request`. The indexer request log events also contain the label `indexer_request`.

## indexer status

- Indexers that are not selected due to `NoStatus`, often have an associated `indexer_status_err` log entriy. These often take the form of:
  - `IndexerVersionError`: failed to query indexer version (often the first request to fail, resulting in the indexer being considered "unavailable")
  - `IndexerStatusError`: failed to query indexing status
