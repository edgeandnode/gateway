use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{anyhow, bail, ensure};
use ordered_float::NotNan;
use serde::Deserialize;
use serde_with::serde_as;
use thegraph_core::SubgraphId;
use tokio::sync::watch;

#[derive(Clone, Debug, Default)]
pub struct AuthSettings {
    pub key: String,
    pub user: String,
    pub authorized_subgraphs: Vec<SubgraphId>,
    pub budget_usd: Option<NotNan<f64>>,
}

impl AuthSettings {
    /// Check if the given subgraph is authorized. If the set of authorized subgraphs is empty, then
    /// any subgraph is authorized.
    pub fn is_subgraph_authorized(&self, subgraph: &SubgraphId) -> bool {
        self.authorized_subgraphs.is_empty() || self.authorized_subgraphs.contains(subgraph)
    }

    pub fn is_any_deployment_subgraph_authorized(&self, subgraphs: &[SubgraphId]) -> bool {
        subgraphs
            .iter()
            .any(|subgraph| self.is_subgraph_authorized(subgraph))
    }
}

#[serde_as]
#[derive(Clone, Debug, Default, Deserialize)]
pub struct APIKey {
    pub key: String,
    pub user_address: String,
    pub query_status: QueryStatus,
    #[serde_as(as = "Option<serde_with::TryFromInto<f64>>")]
    #[serde(rename = "max_budget")]
    pub max_budget_usd: Option<NotNan<f64>>,
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

#[derive(Clone)]
pub struct AuthContext {
    /// This is used to disable the payment requirement on testnets. If `true`, all queries will be
    /// checked for the `query_status` of their API key.
    pub payment_required: bool,
    pub api_keys: watch::Receiver<HashMap<String, APIKey>>,
    pub special_api_keys: Arc<HashSet<String>>,
}

impl AuthContext {
    /// Parse an authorization token into its corresponding settings, and check that the query
    /// should be handled.
    pub fn check(&self, token: &str, domain: &str) -> anyhow::Result<AuthSettings> {
        ensure!(!token.is_empty(), "missing API key");
        parse_api_key(token).ok_or_else(|| anyhow!("malformed API key"))?;

        if self.special_api_keys.contains(token) {
            return Ok(AuthSettings {
                key: token.to_string(),
                user: String::new(),
                authorized_subgraphs: vec![],
                budget_usd: None,
            });
        }

        // Note: holding watch::Ref for the rest of the function
        let api_keys = self.api_keys.borrow();

        let api_key = api_keys
            .get(token)
            .ok_or_else(|| anyhow::anyhow!("API key not found"))?;

        match api_key.query_status {
            QueryStatus::Active => (),
            QueryStatus::ServiceShutoff => {
                bail!("payment required for subsequent requests for this API key");
            }
            QueryStatus::MonthlyCapReached => {
                bail!("spend limit exceeded for this API key");
            }
        }

        ensure!(
            is_domain_authorized(api_key.domains.as_slice(), domain),
            "domain not authorized by user"
        );

        Ok(AuthSettings {
            key: api_key.key.clone(),
            user: api_key.user_address.clone(),
            authorized_subgraphs: api_key.subgraphs.clone(),
            budget_usd: api_key.max_budget_usd,
        })
    }
}

fn parse_api_key(token: &str) -> Option<[u8; 16]> {
    if token.len() != 32 {
        return None;
    }
    let mut buf = [0_u8; 16];
    faster_hex::hex_decode(token.as_bytes(), &mut buf).ok()?;
    Some(buf)
}

/// Check if the query origin domain is authorized.
///
/// If the authorized domain starts with a `*`, it is considered a wildcard domain. This means that
/// any origin domain that ends with the wildcard domain is considered authorized.
///
/// If the authorized domains set is empty, all domains are considered authorized.
pub fn is_domain_authorized<S: AsRef<str>>(authorized: &[S], origin: &str) -> bool {
    fn match_domain(pattern: &str, origin: &str) -> bool {
        if pattern.starts_with('*') {
            origin.ends_with(pattern.trim_start_matches('*'))
        } else {
            origin == pattern
        }
    }

    authorized.is_empty()
        || authorized
            .iter()
            .any(|pattern| match_domain(pattern.as_ref(), origin))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::hex;

    use super::{is_domain_authorized, parse_api_key};

    #[test]
    fn parse_invalid_length_api_key() {
        assert_eq!(parse_api_key("0123456789abcdef0123456789abcde"), None);
    }

    #[test]
    fn parse_invalid_format_api_key() {
        assert_eq!(parse_api_key("abcdefghijklmnopqrstuvwxyz123456"), None);
    }

    #[test]
    fn parse_valid_api_key() {
        assert_eq!(
            parse_api_key("0123456789abcdef0123456789abcdef"),
            Some(hex!("0123456789abcdef0123456789abcdef"))
        );
    }

    #[test]
    fn authorized_domains() {
        let authorized_domains = [
            "example.com",
            "localhost",
            "a.b.c",
            "*.d.e",
            "*-foo.vercel.app",
        ];

        let sub_cases = [
            ("", false),
            ("example.com", true),
            ("subdomain.example.com", false),
            ("localhost", true),
            ("badhost", false),
            ("a.b.c", true),
            ("c", false),
            ("b.c", false),
            ("d.b.c", false),
            ("a", false),
            ("a.b", false),
            ("e", false),
            ("d.e", false),
            ("z.d.e", true),
            ("-foo.vercel.app", true),
            ("foo.vercel.app", false),
            ("bar-foo.vercel.app", true),
            ("bar.foo.vercel.app", false),
        ];

        for (input, expected) in sub_cases {
            assert_eq!(
                expected,
                is_domain_authorized(&authorized_domains, input),
                "match '{input}'"
            );
            // check all authorized when authorized set is empty
            assert!(is_domain_authorized(&[] as &[&str], input));
        }
    }
}
