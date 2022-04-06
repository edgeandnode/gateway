// This list should not be necessary, but it is a temporary measure to avoid unattestable errors
// from getting to users.
// Derived from https://github.com/graphprotocol/graph-node/blob/master/graph/src/data/query/error.rs
pub const UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS: [&'static str; 21] = [
    "Null value resolved for non-null field",     // NonNullError
    "Non-list value resolved for list field",     // ListValueError
    "Failed to get entities from store:",         // ResolveEntitiesError
    "argument must be between 0 and",             // RangeArgumentsError
    "Failed to decode",                           // ValueParseError
    "Broken entity found in store:",              // EntityParseError
    "Store error:",                               // StoreError
    "Query timed out",                            // Timeout
    "Failed to coerce value",                     // EnumCoercionError, ScalarCoercionError
    "Ambiguous result for derived field",         // AmbiguousDerivedFromResult
    "Possible solutions are reducing the depth",  // TooComplex
    "query has a depth that exceeds the limit",   // TooDeep
    "query resolution yielded different results", // IncorrectPrefetchResult
    "panic processing query:",                    // Panic
    "error in the subscription event stream",     // EventStreamError
    "query is too expensive",                     // TooExpensive
    "service is overloaded and can not run",      // Throttled
    "the chain was reorganized while executing",  // DeploymentReverted
    "failed to resolve subgraph manifest:",       // SubgraphManifestResolveError
    "invalid subgraph manifest file",             // InvalidSubgraphManifest
    "is larger than the allowed limit of",        // ResultTooBig
                                                  // TODO: ValidationError
];
