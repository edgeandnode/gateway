# Middleware Module

Axum HTTP middleware layers for request processing.

## Module Overview

| File                 | Purpose                                  |
| -------------------- | ---------------------------------------- |
| `request_tracing.rs` | Adds request ID and tracing span         |
| `require_auth.rs`    | Enforces API key authentication          |
| `legacy_auth.rs`     | Adapts legacy `/{api_key}/...` URL paths |

## Middleware Stack

Applied top-to-bottom for each request:

```
Request
    |
    v
+---------------------------+
| CorsLayer                 |  Allow cross-origin requests
+---------------------------+
    |
    v
+---------------------------+
| RequestTracingLayer       |  Generate request_id, create span
+---------------------------+
    |
    v
+---------------------------+
| legacy_auth_adapter       |  Move API key from path to header
+---------------------------+
    |
    v
+---------------------------+
| RequireAuthorizationLayer |  Validate key, add AuthSettings
+---------------------------+
    |
    v
Handler
```

## Request Tracing

Each request gets a unique `request_id` (UUID v4) added to the tracing span.
The span includes:

- Request method and path
- Response status
- Duration

## Legacy Auth Adapter

Converts legacy URL format to modern header-based auth:

```
/api/{api_key}/subgraphs/id/{id}
    |
    v
/api/subgraphs/id/{id}
+ Authorization: Bearer {api_key}
```

## Authorization Layer

Validates API keys and adds `AuthSettings` to request extensions:

```rust
// In handler
let auth: Extension<AuthSettings> = ...;
if !auth.is_subgraph_authorized(&subgraph_id) {
    return Err(Error::Auth(...));
}
```
