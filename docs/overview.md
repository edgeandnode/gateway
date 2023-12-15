# Overview

At a high level the gateway does 2 things:

1. Route client requests to indexers
2. Facilitate payments to indexers

## Query Lifecycle

1. The client GraphQL request arrives, including an auth token (API key or query key). The auth token is used to check associated allowlists, payment status, etc.
2. Indexers are selected from the set allocated to the subgraph deployment being queried. For queries by subgraph ID (GNS ID), indexers are selected across the allocations on all associated deployments (subgraph versions).
3. A subset of up to 3 indexers are selected based on a variety of selection factors including reliability, latency, subgraph version (if applicable), etc.
4. The request is made deterministic by replacing block numbers with hashes for the chain being indexed by the subgraph.
5. The request is forwarded to each selected indexer.
6. Each indexer’s response, latency, etc. is fed back into indexer selection.
7. The first valid indexer response is returned it to the client. If no indexers return a valid response goto step 3.

## Design Principles

- The gateway is designed to be a reliable system, to compensate for indexers being relatively unreliable. Indexers may become unresponsive, malicious, or otherwise unsuitable for serving queries at any time without warning. It is the responsibility of the gateway to maintain the highest possible quality of service(QoS) under these conditions.

- The gateway's primary responsibilities are to serve client requests and to facilitate indexer payments. Other responsibilities, though important, are secondary and therefore their failure modes must have minimal impact on the primary responsibilities.
