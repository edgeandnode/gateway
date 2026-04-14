# The Graph Gateway

Query blockchain data from The Graph Network's decentralized indexers.

## Environments

| Environment | Base URL |
|-------------|----------|
| Mainnet | `https://gateway.thegraph.com` |
| Testnet | `https://testnet.gateway.thegraph.com` |

## Authentication

Two options for accessing the API:

### Option 1: API Key (best for humans)

Get an API key from [Subgraph Studio](https://thegraph.com/studio) and include it in requests.

**Endpoints:**
- `POST /api/subgraphs/id/{subgraph_id}`
- `POST /api/deployments/id/{deployment_id}`

**Header:** `Authorization: Bearer <API_KEY>`

### Option 2: x402 Payment (best for agents)

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

### With x402 Payment

Any x402 tooling that supports exact scheme will work with the gateway's x402 endpoints. We recommend to use the official Graph x402 client:

```bash
npm install @graphprotocol/client-x402
```

**Option A: Command Line**

```bash
export X402_PRIVATE_KEY=0xabc123...

npx graphclient-x402 "{ pairs(first: 5) { id } }" \
  --endpoint https://gateway.thegraph.com/api/x402/subgraphs/id/<SUBGRAPH_ID> \
  --chain base
```

**Option B: Programmatic**

```typescript
import { createGraphQuery } from '@graphprotocol/client-x402'

const query = createGraphQuery({
  endpoint: 'https://gateway.thegraph.com/api/x402/subgraphs/id/<SUBGRAPH_ID>',
  chain: 'base',
})

const result = await query('{ pairs(first: 5) { id } }')
```

**Option C: Typed SDK (full type safety)**

```bash
npm install @graphprotocol/client-cli @graphprotocol/client-x402
```

Configure `.graphclientrc.yml`:

```yaml
customFetch: '@graphprotocol/client-x402'

sources:
  - name: uniswap
    handler:
      graphql:
        endpoint: https://gateway.thegraph.com/api/x402/subgraphs/id/<SUBGRAPH_ID>

documents:
  - ./src/queries/*.graphql
```

Build and use:

```bash
export X402_PRIVATE_KEY=0xabc123...
export X402_CHAIN=base
npx graphclient build
```

```typescript
import { execute, GetPairsDocument } from './.graphclient'

const result = await execute(GetPairsDocument, { first: 5 })
```

**Environment Variables:**
- `X402_PRIVATE_KEY`: Wallet private key for payment signing
- `X402_CHAIN`: `base` (mainnet) or `base-sepolia` (testnet)