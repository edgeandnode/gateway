use alloy_primitives::Address;

/// Rate limit settings.
///
/// The settings can be provided globally (e.g., via config) or via authorization
/// (e.g., subscription specific rate).
#[derive(Clone, Debug, Default)]
pub struct RateLimitSettings {
    /// The rate limit key.
    pub key: Address,
    /// The query rate in queries per minute.
    pub queries_per_minute: usize,
}
