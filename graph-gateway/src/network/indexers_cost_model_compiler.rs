//! Cost model compiler with compilation cache.
//!
//! The cost models fetched from the indexer's cost URL are compiled into cost models. The
//! compilation results are cached, so if the same cost model source is compiled multiple times,
//! the compilation result is returned from the cache.
//!
//! By default, the cost model compilation cache entries expire after 12 hours.

use std::time::Duration;

use cost_model::{CompileError, CostModel};
use eventuals::Ptr;
use gateway_common::ttl_hash_map::TtlHashMap;

use crate::indexers::cost_models::CostModelSource;

/// Default time-to-live for the cost model compilation cache entries: 12 hours.
const DEFAULT_COMPILATION_CACHE_TTL: Duration = Duration::from_secs(12 * 60 * 60);

/// Internal representation of a cost model source to be used as a key in the compilation cache
/// hashmap.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CostModelSrc {
    model: String,
    variables: Option<String>,
}

/// Error type for cost model compilation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CompilationError {
    /// The cost model is too large.
    #[error("Cost model compilation failed: size too large: {0}")]
    CostModelTooLarge(usize),
    /// The cost model document parsing failed.
    #[error("Cost model compilation failed: document parsing failed: {0}")]
    DocumentParsingFailed(String),
    /// The cost model globals parsing failed.
    #[error("Cost model compilation failed: globals parsing failed: {0}")]
    GlobalsParsingFailed(String),
    /// Unknown compilation error.
    #[error("Cost model compilation failed: unknown error")]
    Unknown,
}

impl From<CompileError> for CompilationError {
    fn from(value: CompileError) -> CompilationError {
        match value {
            CompileError::DocumentParseError(err) => CompilationError::DocumentParsingFailed(err),
            CompileError::GlobalsParseError(err) => {
                CompilationError::GlobalsParsingFailed(err.to_string())
            }
            CompileError::Unknown => CompilationError::Unknown,
        }
    }
}

/// Resolve the indexers' cost models sources and compile them into cost models.
pub struct CostModelCompiler {
    cache: TtlHashMap<CostModelSrc, Result<Ptr<CostModel>, CompilationError>>,
}

impl Default for CostModelCompiler {
    /// Creates a new [`CostModelCompiler`] instance with the default compilation cache
    /// time-to-live, which is 12 hours.
    fn default() -> Self {
        Self {
            cache: TtlHashMap::with_ttl(DEFAULT_COMPILATION_CACHE_TTL),
        }
    }
}

impl CostModelCompiler {
    /// Creates a new [`CostModelCompiler`] instance with a custom compilation cache time-to-live.
    pub fn new(cache_ttl: Duration) -> Self {
        Self {
            cache: TtlHashMap::with_ttl(cache_ttl),
        }
    }

    /// Compile a cost model from sources.
    ///
    /// The compilation result is cached, so if the same cost model source is compiled multiple
    /// times, the compilation result is returned from the cache.
    pub fn compile(&mut self, src: CostModelSource) -> Result<Ptr<CostModel>, CompilationError> {
        // Check the cost model source size
        if src.model.len() > (1 << 16) {
            return Err(CompilationError::CostModelTooLarge(src.model.len()));
        }

        // Construct the cost model source representation
        let src = CostModelSrc {
            model: src.model,
            variables: src.variables,
        };

        // Check the cache for the compilation result, if it exists, return it. Otherwise, compile
        // the cost model and cache the compilation result.
        match self.cache.get(&src) {
            Some(compilation_result) => compilation_result.clone(),
            None => {
                let compilation_sources = src.clone();
                let compilation_result =
                    CostModel::compile(&src.model, &src.variables.unwrap_or_default())
                        .map(Ptr::new)
                        .map_err(Into::into);

                // Cache the compilation result
                self.cache
                    .insert(compilation_sources, compilation_result.clone());

                compilation_result
            }
        }
    }
}
