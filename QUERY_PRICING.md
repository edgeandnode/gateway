# Query Pricing Model

This document explains how query pricing works in The Graph Gateway, from when a query arrives to how indexers get paid.

## Overview

The gateway routes GraphQL queries to indexers and pays them using TAP (Timeline Aggregation Protocol) receipts. The pricing system balances three goals:

1. **Meet spending targets** - The gateway aims to spend a configured USD amount per query
2. **Prefer cheap indexers** - Lower-cost indexers are favored in selection
3. **Ensure minimum payments** - A dynamic floor ensures indexers receive meaningful compensation

## Query Budget

When a query arrives, the gateway calculates a budget in GRT (Graph Tokens):

```
budget = query_fees_target (USD) × grt_per_usd × 10¹⁸
```

- **query_fees_target**: A USD amount configured by the gateway operator (e.g., $0.00012 per query)
- **grt_per_usd**: Live exchange rate from an external price feed
- **10¹⁸**: Converts GRT to wei (smallest unit)

This budget serves as a reference point for comparing indexer costs, not a hard spending limit.

## Indexer Cost Models

Each indexer declares their price by exposing a cost model at their `/cost` endpoint:

```
default => 0.004100;
```

This value represents the fee in GRT that the indexer wants per query. The gateway fetches and caches these cost models periodically. If an indexer doesn't provide a cost model, their fee defaults to zero.

## Fee Normalization

To compare indexers fairly, fees are normalized against the query budget:

```
normalized_fee = indexer_fee / budget
```

This produces a value where:
- **0.0** = free indexer
- **0.5** = indexer charges half the budget
- **1.0** = indexer charges the full budget (or more)

Values exceeding 1.0 are capped at 1.0. This means indexers who price themselves above the budget are treated the same as those at exactly the budget level.

## Indexer Selection

The gateway selects up to 3 indexers per query. Selection considers multiple factors:

- **Success rate**: Historical query success percentage
- **Latency**: Response time performance
- **Chain sync status**: How up-to-date the indexer's data is
- **Staked amount**: Economic security (slashable stake)
- **Normalized fee**: Cost relative to budget

The selection algorithm ranks candidates by:

```
ranking = marginal_score / fee
```

This means a cheap indexer (fee = 0.01) gets approximately 100x better ranking than an expensive one (fee = 1.0), assuming equal scores. Low fees significantly improve an indexer's chances of being selected.

## Dynamic Minimum Fees

The gateway uses a PID (Proportional-Integral-Derivative) controller to maintain spending near the target. This creates a dynamic minimum fee that adjusts based on actual spending:

1. Every second, the controller calculates the average fees actually paid
2. It compares this to the target spending rate
3. If spending is below target, the minimum fee increases
4. If spending is above target, the minimum fee decreases

The minimum fee is bounded between a floor (approximately $0.00001) and the configured target.

## Actual Payment Calculation

For each selected indexer, the actual payment is:

```
payment = max(normalized_fee × budget, dynamic_minimum / number_of_indexers)
```

This ensures:
- Indexers receive at least what they asked for (up to the budget)
- The gateway spends at least the dynamic minimum across all selected indexers
- Payments are distributed to help meet the spending target

## Payment Caps

The gateway protects against excessive costs:

- **Normalized fee cap**: Fees are capped at 1.0 (the budget), regardless of what the indexer declares
- **Selection penalty**: High fees result in much worse selection ranking

If an indexer declares a fee of 10 GRT but the budget is 0.001 GRT, they:
- Get normalized fee of 1.0 (capped)
- Receive at most the budget amount if selected
- Are unlikely to be selected unless they're the only option

## TAP Receipts

Payment is made via TAP receipts, which are cryptographic proofs of payment:

- Created before sending the query to each indexer
- Contain: payment amount, payer address, indexer address, timestamp, allocation ID
- Signed by the gateway using EIP-712
- Sent to the indexer as an HTTP header with the query

Indexers collect these receipts and redeem them through the protocol's payment channels.

## Multi-Indexer Queries

The gateway queries up to 3 indexers in parallel for each client request:

- **First successful response wins**: The client receives the first valid response
- **All queried indexers get paid**: Every indexer that received a receipt is paid, regardless of whether their response was used
- **Late responses are discarded**: Additional successful responses after the first are ignored

This design incentivizes fast responses while ensuring indexers are compensated for their work.

## Feedback Loop

After each query completes, the total fees paid (sum of all receipts) are reported back to the budget controller. This feedback drives the PID controller's adjustments, creating a closed loop that converges toward the target spending rate over time.

## Summary

| Component | Purpose |
|-----------|---------|
| Query budget | Reference point for normalizing indexer fees |
| Normalized fees | Fair comparison of indexer costs (0-1 scale) |
| Selection algorithm | Picks best indexers considering cost and performance |
| Dynamic minimum | Ensures spending meets targets even with free indexers |
| Payment cap | Protects against excessive costs (capped at budget) |
| TAP receipts | Cryptographic proof of payment sent with queries |
| Feedback loop | Adjusts minimum fees to hit spending targets |

The system creates a market where cheap, performant indexers are preferred, but all participating indexers receive fair compensation for their services.
