# The Graph Gateway

Query blockchain data from The Graph Network's decentralized indexers.

## Environments

| Environment | Base URL |
|-------------|----------|
| Mainnet | `https://gateway.thegraph.com` |
| Testnet | `https://testnet.gateway.thegraph.com` |

## Authentication

Two options for accessing the API:

### Option 1: API Key

Get an API key from [Subgraph Studio](https://thegraph.com/studio) and include it in requests.

**Endpoints:**
- `POST /api/subgraphs/id/{subgraph_id}`
- `POST /api/deployments/id/{deployment_id}`

**Header:** `Authorization: Bearer <API_KEY>`

### Option 2: x402 Payment

Pay per query with USDC. No API key required.

**Endpoints:**
- `POST /api/x402/subgraphs/id/{subgraph_id}`
- `POST /api/x402/deployments/id/{deployment_id}`

## Examples

### With API Key

```bash
curl -X POST https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ tokens(first: 5) { symbol } }"}'
```

### With x402 (using x402-proxy cli)

```bash
X402_PROXY_WALLET_EVM_KEY="<PAYER_WALLET_PRIVATE_KEY>" \
npx x402-proxy http://gateway.thegraph.com/api/x402/deployments/id/QmXU9FEf1tUwSjsnGGsuvMHFmGq3CeEi1RiWrNXSQXzkAi \
  -X POST \
  --header 'content-type: application/json' \
  -d '{"query":"{ indexers { id stakedTokens allocations { id } } }"}'
```