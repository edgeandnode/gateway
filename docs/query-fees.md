# Query Fees

## Initial Notes

- In most contexts a "query" or "client query", refers to a single GraphQL HTTP request from the client. However, [Agora](https://github.com/graphprotocol/agora) cost models define a query as a top level selection for the operation being executed ([spec](https://spec.graphql.org/October2021/#sec-Selection-Sets)). The rationale is that this roughly translates to the amount of SQL queries made by graph-node to execute the query, at the time Agora was designed. This may seem like a useful measure for a query's computational complexity, but is has been shown to be practically unrelated to "real" query cost.

## Introduction

The gateway serves its budget per client query, in USD, at `/budget`. Indexers make their prices available via Agora cost-models. These cost models are served, for each subgraph deployment, by indexer-service at `/cost`. When selecting indexers, the gateway first executes their cost models over the client query to obtain each indexer's fee. Indexer selection will favor indexers with lower fees, all else being equal. Indexer fees are clamped to a maximum of the gateway's budget.

## Implementation Details

The gateway has a control system that may pay indexers more than they request via their cost models in an effort to hit an average of `budget` fees per client query.
