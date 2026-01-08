# ADR-001: Static Allocations via Box::leak

## Status

Accepted

## Context

The graph-gateway uses Axum as its HTTP framework. Axum's state management requires types to implement `Clone` and have `'static` lifetime. Several gateway components are heavyweight singletons that:

1. Are initialized once at startup
2. Never need to be deallocated (process lifetime)
3. Are expensive to clone (contain channels, cryptographic keys, etc.)

These components include:

- `ReceiptSigner` - TAP receipt signing with private keys
- `Budgeter` - PID controller state for fee management
- `Chains` - Chain head tracking with per-chain state
- `Eip712Domain` (attestation domains) - EIP-712 signing domains

## Decision

Use `Box::leak()` to convert owned `Box<T>` into `&'static T` references for singleton components.

```rust
// Example from main.rs
let receipt_signer: &'static ReceiptSigner = Box::leak(Box::new(ReceiptSigner::new(...)));

let chains: &'static Chains = Box::leak(Box::new(Chains::new(...)));
```

## Consequences

### Positive

1. **Zero-cost sharing**: `&'static T` is `Copy`, so passing to handlers has no overhead
2. **No Arc overhead**: Avoids atomic reference counting on every request
3. **Simpler lifetimes**: No need to propagate lifetime parameters through handler types
4. **Explicit intent**: Makes it clear these are process-lifetime singletons

### Negative

1. **Memory never freed**: The leaked memory is never reclaimed. Acceptable because:
   - Components live for the entire process lifetime anyway
   - Total leaked memory is small and bounded (< 1 KB)
   - Process termination reclaims all memory

2. **Not suitable for tests**: Tests that need fresh state must use different patterns. Currently mitigated by limited test coverage.

## Alternatives Considered

### `Arc<T>` (Rejected)

```rust
let receipt_signer: Arc<ReceiptSigner> = Arc::new(ReceiptSigner::new(...));
```

Problems:

- Atomic operations on every clone (per-request overhead)
- More complex to share across Axum handlers
- Implies shared ownership when sole ownership is the intent

### `once_cell::sync::Lazy` (Rejected)

```rust
static RECEIPT_SIGNER: Lazy<ReceiptSigner> = Lazy::new(|| ...);
```

Problems:

- Requires initialization logic in static context
- Cannot use async initialization
- Configuration not available at static init time

## References

- [Axum State Documentation](https://docs.rs/axum/latest/axum/extract/struct.State.html)
- [Box::leak documentation](https://doc.rust-lang.org/std/boxed/struct.Box.html#method.leak)
