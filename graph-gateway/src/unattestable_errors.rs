// This list should not be necessary, but it is a temporary measure to avoid unattestable errors
// from getting to users.
// Derived from https://github.com/graphprotocol/graph-node/blob/master/graph/src/data/query/error.rs
#[rustfmt::skip]
pub const UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS: [&str; 22] = [
    "Non-list value resolved for list field",     // ListValueError
    "Failed to get entities from store:",         // ResolveEntitiesError
    "argument must be between 0 and",             // RangeArgumentsError
    "Broken entity found in store:",              // EntityParseError
    "Store error:",                               // StoreError
    "Query timed out",                            // Timeout
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

    // graph-node features
    "\"block__timestamp\" does not exist",                                       // v0.28.0
    "Invalid value provided for argument `orderBy`: Enum(\"block__timestamp\")", // v0.28.0
    "ield \"and\" is not defined by type",                                       // v0.30.0
    "ield \"or\" is not defined by type",                                        // v0.30.0
];

// Note: The linked PRs may be merged. But these must still be special-cased unless the minimum
// indexer version includes these fixes.
#[rustfmt::skip]
pub const MISCATEGORIZED_ATTESTABLE_ERROR_MESSAGE_FRAGMENTS: [&str; 4] = [
    "Null value resolved for non-null field",     // NonNullError (https://github.com/graphprotocol/graph-node/pull/3507)
    "Failed to decode",                           // ValueParseError (https://github.com/graphprotocol/graph-node/pull/4278)
    "Failed to coerce value",                     // EnumCoercionError, ScalarCoercionError (https://github.com/graphprotocol/graph-node/pull/4278)
    "Child filters can not be nested",            // StoreError (https://github.com/graphprotocol/graph-node/issues/4775)
];

pub fn miscategorized_unattestable(error: &str) -> bool {
    let mut unattestable = UNATTESTABLE_ERROR_MESSAGE_FRAGMENTS
        .iter()
        .any(|err| error.contains(err));

    let and_or_filter_err = error.contains("Invalid value provided for argument `where`:")
        && (error.contains("{\"or\":") || error.contains("{\"and\":"));
    unattestable |= and_or_filter_err;

    unattestable && !miscategorized_attestable(error)
}

pub fn miscategorized_attestable(error: &str) -> bool {
    MISCATEGORIZED_ATTESTABLE_ERROR_MESSAGE_FRAGMENTS
        .iter()
        .any(|err| error.contains(err))
}

#[cfg(test)]
mod test {
    use super::miscategorized_unattestable;

    #[test]
    fn unsupported_or_filter() {
        let error = "Invalid value provided for argument `where`: Object({\"or\": List([Object({\"state\": Enum(\"Active\"), \"utilization_gte\": String(\"10000000000\")}), Object({\"state\": Enum(\"Created\")}), Object({\"createdAt_gt\": Int(Number(1708867002))})])})";
        assert!(miscategorized_unattestable(error));
    }
}
