# ADR-002: Type-State Pattern for Indexer Processing

## Status

Accepted

## Context

Indexer information flows through multiple processing stages, with each stage enriching the data:

1. **Raw** - Basic indexer info from network subgraph
2. **Version resolved** - After fetching indexer-service version
3. **Progress resolved** - After fetching indexing progress (block height)
4. **Cost resolved** - After fetching cost model/fee info

Processing order matters: we need version info before we can query for progress (different API versions), and we need progress before cost resolution makes sense (stale indexers are filtered).

A naive approach would use `Option<T>` fields that get populated:

```rust
struct IndexingInfo {
    indexer: IndexerId,
    deployment: DeploymentId,
    version: Option<Version>,      // Filled in stage 2
    progress: Option<BlockNumber>, // Filled in stage 3
    fee: Option<GRT>,              // Filled in stage 4
}
```

This leads to `unwrap()` calls throughout the codebase and runtime errors when accessing fields before they're populated.

## Decision

Use the type-state pattern with generic parameters to encode processing stage at compile time.

```rust
// Type markers for processing stages
struct Unresolved;
struct VersionResolved(Version);
struct ProgressResolved { version: Version, block: BlockNumber }
struct FullyResolved { version: Version, block: BlockNumber, fee: GRT }

// Generic struct parameterized by stage
struct IndexingInfo<Stage> {
    indexer: IndexerId,
    deployment: DeploymentId,
    stage: Stage,
}

// Stage transitions are explicit methods
impl IndexingInfo<Unresolved> {
    fn resolve_version(self, version: Version) -> IndexingInfo<VersionResolved> {
        IndexingInfo {
            indexer: self.indexer,
            deployment: self.deployment,
            stage: VersionResolved(version),
        }
    }
}
```

See `src/network/indexer_processing.rs` for the actual implementation.

## Consequences

### Positive

1. **Compile-time safety**: Impossible to access version info before it's resolved
2. **Self-documenting**: Function signatures show required processing stage
3. **No runtime overhead**: Type parameters are erased at compile time
4. **Explicit transitions**: Stage changes are visible method calls, not silent mutations

### Negative

1. **Verbose types**: `IndexingInfo<ProgressResolved>` is longer than `IndexingInfo`
2. **Learning curve**: Pattern is less common, may confuse new contributors
3. **More boilerplate**: Stage transition methods must be written explicitly

## Pattern Usage

```rust
// Functions declare their required stage in the signature
fn select_candidate(info: &IndexingInfo<FullyResolved>) -> Score {
    // Safe to access info.stage.fee - compiler guarantees it exists
    calculate_score(info.stage.fee, info.stage.block)
}

// Processing pipeline
async fn process_indexer(raw: IndexingInfo<Unresolved>) -> Result<IndexingInfo<FullyResolved>> {
    let with_version = raw.resolve_version(fetch_version(&raw.indexer).await?);
    let with_progress = with_version.resolve_progress(fetch_progress(&with_version).await?);
    let fully_resolved = with_progress.resolve_cost(fetch_cost(&with_progress).await?);
    Ok(fully_resolved)
}
```

## Alternatives Considered

### Builder Pattern (Rejected)

```rust
IndexingInfoBuilder::new(indexer, deployment)
    .version(v)
    .progress(p)
    .fee(f)
    .build()
```

Problems:

- Runtime validation only
- `build()` must check all fields are set
- No compile-time guarantee of processing order

### Separate Structs (Rejected)

```rust
struct RawIndexingInfo { ... }
struct ResolvedIndexingInfo { ... }
```

Problems:

- Code duplication across struct definitions
- Harder to share common logic
- Type relationships not explicit

## References

- [Typestate Pattern in Rust](https://cliffle.com/blog/rust-typestate/)
- [Parse, don't validate](https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/)
