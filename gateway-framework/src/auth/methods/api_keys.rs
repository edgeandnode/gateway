use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::Address;
use ordered_float::NotNan;
use serde::Deserialize;
use serde_with::serde_as;
use thegraph_core::types::{DeploymentId, SubgraphId};
use tokio::sync::watch;

use super::common;
use crate::{auth::QuerySettings, http::middleware::RateLimitSettings};

#[serde_as]
#[derive(Clone, Debug, Default, Deserialize)]
pub struct APIKey {
    pub key: String,
    pub user_address: Address,
    pub query_status: QueryStatus,
    #[serde_as(as = "Option<serde_with::TryFromInto<f64>>")]
    #[serde(rename = "max_budget")]
    pub max_budget_usd: Option<NotNan<f64>>,
    #[serde(default)]
    pub deployments: Vec<DeploymentId>,
    #[serde(default)]
    pub subgraphs: Vec<SubgraphId>,
    #[serde(default)]
    pub domains: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryStatus {
    #[default]
    Active,
    ServiceShutoff,
    MonthlyCapReached,
}

/// Errors that may occur when parsing an API key.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid length: {0}")]
    InvalidLength(usize),

    #[error("invalid format: {0}")]
    InvalidHex(faster_hex::Error),
}

/// Parses an API key from a string.
///
/// The API key is expected to be a 32-character hex string.
pub fn parse_api_key(value: &str) -> Result<[u8; 16], ParseError> {
    if value.len() != 32 {
        return Err(ParseError::InvalidLength(value.len()));
    }

    let mut buf = [0_u8; 16];
    faster_hex::hex_decode(value.as_bytes(), &mut buf).map_err(ParseError::InvalidHex)?;
    Ok(buf)
}

/// Auth token wrapper around an API key.
#[derive(Debug, Clone)]
pub struct AuthToken {
    /// The API key.
    api_key: Arc<APIKey>,
}

impl AuthToken {
    /// Create a new auth token from the given API key.
    pub fn new(api_key: Arc<APIKey>) -> Self {
        Self { api_key }
    }

    /// Get the API key user address.
    pub fn user(&self) -> Address {
        self.api_key.user_address
    }

    /// Get the API key string.
    pub fn key(&self) -> &str {
        &self.api_key.key
    }

    /// Get the API key.
    pub fn api_key(&self) -> &APIKey {
        &self.api_key
    }

    /// Check if the given domain is authorized by the API key.
    pub fn is_domain_authorized(&self, domain: &str) -> bool {
        let allowed_domains = &self
            .api_key
            .domains
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>();

        common::is_domain_authorized(allowed_domains, domain)
    }

    /// Check if the given subgraph is authorized by the API key.
    pub fn is_subgraph_authorized(&self, subgraph: &SubgraphId) -> bool {
        let allowed_subgraphs = &self.api_key.subgraphs;
        common::is_subgraph_authorized(allowed_subgraphs, subgraph)
    }

    /// Check if the given deployment is authorized by the API key.
    pub fn is_deployment_authorized(&self, deployment: &DeploymentId) -> bool {
        let allowed_deployments = &self.api_key.deployments;
        common::is_deployment_authorized(allowed_deployments, deployment)
    }

    /// Check if ALL subgraphs are authorized by the API key.
    pub fn are_subgraphs_authorized(&self, subgraphs: &[SubgraphId]) -> bool {
        let allowed_subgraphs = &self.api_key.subgraphs;
        common::are_subgraphs_authorized(allowed_subgraphs, subgraphs)
    }

    /// Check if ALL deployments are authorized by the API key.
    pub fn are_deployments_authorized(&self, deployments: &[DeploymentId]) -> bool {
        let allowed_deployments = &self.api_key.deployments;
        common::are_deployments_authorized(allowed_deployments, deployments)
    }
}

impl std::fmt::Display for AuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.api_key.key)
    }
}

/// App state (a.k.a [Context](crate::client_query::Context)) sub-state.
pub struct AuthContext {
    /// A map between auth bearer token string and the [ApiKey].
    pub(crate) api_keys: watch::Receiver<HashMap<String, Arc<APIKey>>>,

    /// Special API keys that don't require payment.
    ///
    /// An API key is considered special when does not require payment and is
    /// not subsidized, i.e., these keys won't be rejected due to non-payment.
    pub(crate) special_api_keys: Arc<HashSet<String>>,
}

impl AuthContext {
    /// Get the API key associated with the given bearer token string.
    pub fn get_api_key(&self, token: &str) -> Option<Arc<APIKey>> {
        self.api_keys.borrow().get(token).cloned()
    }

    /// Check if the given API key is a special key.
    ///
    /// An API key is considered special when does not require payment and is not subsidized, i.e., these
    /// keys won't be rejected due to non-payment.
    pub fn is_special_key(&self, api_key: &APIKey) -> bool {
        self.special_api_keys.contains(&api_key.key)
    }
}

/// Parse the bearer token as a API key and retrieve the associated API key.
pub fn parse_auth_token(
    ctx: &AuthContext,
    token: &str,
) -> anyhow::Result<(AuthToken, Option<QuerySettings>, Option<RateLimitSettings>)> {
    // Check if the bearer token is a valid 32 hex digits key
    if parse_api_key(token).is_err() {
        return Err(anyhow::anyhow!("invalid api key format"));
    }

    // Retrieve the API Key associated with the bearer token
    let api_key = &ctx
        .get_api_key(token)
        .ok_or_else(|| anyhow::anyhow!("api key not found"))?;

    // Build the query settings struct
    let query_settings = QuerySettings {
        budget_usd: api_key.max_budget_usd,
    };

    Ok((AuthToken::new(api_key.clone()), Some(query_settings), None))
}

/// Perform API key auth token specific requirements checks.
///
/// Checks performed:
///  1. Check if the API key is a special key.
///  2. Check if the API key is subsidized.
///  3. Check if the API key is active.
pub fn check_auth_requirements(ctx: &AuthContext, token: &AuthToken) -> anyhow::Result<()> {
    // Check if the API key is a special key
    if ctx.is_special_key(token.api_key()) {
        return Ok(());
    }

    // Check if the API key is active
    match token.api_key.query_status {
        QueryStatus::Active => Ok(()),
        QueryStatus::ServiceShutoff => Err(anyhow::anyhow!(
            "payment required for subsequent requests for this API key"
        )),
        QueryStatus::MonthlyCapReached => {
            Err(anyhow::anyhow!("spend limit exceeded for this API key"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod parser {
        use assert_matches::assert_matches;

        use super::{parse_api_key, ParseError};

        #[test]
        fn parse_invalid_length_api_key() {
            //* Given
            let api_key = "0123456789abcdef0123456789abcde";

            //* When
            let result = parse_api_key(api_key);

            //* Then
            assert_matches!(result, Err(ParseError::InvalidLength(31)));
        }

        #[test]
        fn parse_invalid_format_api_key() {
            //* Given
            let api_key = "abcdefghijklmnopqrstuvwxyz123456";

            //* When
            let result = parse_api_key(api_key);

            //* Then
            assert_matches!(result, Err(ParseError::InvalidHex(_)));
        }

        #[test]
        fn parse_valid_api_key() {
            //* Given
            let api_key = "0123456789abcdef0123456789abcdef";

            //* When
            let result = parse_api_key(api_key);

            //* Then
            assert_matches!(result, Ok(key) => {
                assert_eq!(
                    key,
                    [
                        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                    ]
                );
            });
        }
    }
}
