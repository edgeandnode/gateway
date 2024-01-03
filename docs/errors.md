# Errors

Error messages are given to users when the gateway is unable to return a "suitable" response, from an indexer, to the client.

Each specific error that be returned to the client when making a subgraph query is defined in [errors.rs](../gateway-framework/src/errors.rs) (`gateway_framework::errors::Error`).

Under normal conditions, outside of user error, regressions in gateway performance may be identified using the details provided in the `BadIndexers` error. This message includes a list of indexer errors encountered, in descending order of how many of the potential indexers failed for that reason.
