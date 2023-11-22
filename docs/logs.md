# Logs

## Log Levels

- `error`: An unexpected state has been reached, which is likely to have a negative impact on the gateway's ability to serve queries or make payments.
- `warn`: An unexpected state has been reached, though it is recoverable and unlikely to have a negative impact on the gateway's ability to serve queries or make payments.
- `info`: Information that is commonly used to trace the execution of the major gateway subsystems in production.
- `debug`: Similar to `info`, but is often irrelevant when investigating gateway execution in production.
- `trace`: Information that is considered too verbose for production, but is often useful during development.

## client requests

Available log fields:
- `query_id`: identifies the client request.For any request containing a CF-Ray header (Cloudflare Ray ID), the `query_id` will be set to the header value. Filtering log entries by the `query_id` will show all the log events associated with handling the request.
- `selector`: subgraph or deployment ID used in the client request path. Indexer requests are always associated with a `deployment` as well.
- `deployments`: set of relevant deployments
- `subgraph_chain`
- `domain`: value of the request origin header
- `status_message`
- `status_code`
- `user_address`: user address associated with the API key or query key
- `query`
- `variables`
- `query_count`: queries included in the request
- `budget_grt`
- `indexer_fees_grt`

## indexer requests

Available log fields:
- `url`
- `blocks_behind`
- `fee_grt`
- `response_time_ms`
- `status_message`
- `status_code`
- `allocation`
- `indexer_errors`

Note that a client request may be associated with zero or more indexer requests.

## indexer-selection behavior

- With trace logs enabled (`indexer_selection=trace`), indexer selection will always log an `available` event with a dump of selection factors for all the indexers being compared (filter using `|= "indexer_selection" |= "available"`). Otherwise, the same log event is only emitted at debug level approximately once per 1000 client requests.

## indexer status

- Indexers that are not selected due to `NoStatus`, often have an associated `indexer_status_err` log entriy. These often take the form of:
  - `IndexerVersionError`: failed to query indexer version (often the first request to fail, resulting in the indexer being considered "unavailable")
  - `IndexerStatusError`: failed to query indexing status
