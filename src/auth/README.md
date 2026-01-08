# Auth Module

API key authentication and authorization.

## Module Overview

| File            | Purpose                                           |
| --------------- | ------------------------------------------------- |
| `mod.rs`        | `AuthContext`, `AuthSettings`, API key validation |
| `studio_api.rs` | Fetch API keys from HTTP endpoint                 |
| `kafka.rs`      | Stream API keys from Kafka topic                  |

## Authentication Flow

```
Request
    |
    v
Extract API key from Authorization header
    |
    v
Validate format (32-char hex string)
    |
    v
Special key? --> Skip payment check
    |
    v
Look up in api_keys map
    |
    v
Check QueryStatus (Active/ServiceShutoff/MonthlyCapReached)
    |
    v
Verify domain authorization
    |
    v
Return AuthSettings
```

## Key Types

- `AuthContext` - Shared auth state (api_keys map, special keys, payment_required flag)
- `AuthSettings` - Per-request auth info (key, user, authorized subgraphs)
- `ApiKey` - API key definition with permissions
- `QueryStatus` - Payment status enum

## API Key Sources

### Studio API (`studio_api.rs`)

Polls an HTTP endpoint periodically to fetch API keys.

```json
{
  "url": "https://api.example.com/gateway-api-keys",
  "auth": "Bearer <token>",
  "special": ["admin-key-1"]
}
```

### Kafka (`kafka.rs`)

Streams API key updates from a Kafka topic.

```json
{
  "topic": "gateway-api-keys",
  "special": ["admin-key-1"]
}
```

## Domain Authorization

The `domains` field on API keys supports:

- Exact match: `"example.com"`
- Wildcard: `"*.example.com"` (matches `foo.example.com`)
- Empty list: all domains authorized
