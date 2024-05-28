use alloy_primitives::Address;
use anyhow::{anyhow, bail, ensure};
use ordered_float::NotNan;
use serde::Deserialize;
use serde_with::serde_as;
use thegraph_core::types::SubgraphId;

use crate::auth::{AuthContext, AuthSettings};

use super::common::is_domain_authorized;

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

pub fn check(ctx: &AuthContext, token: &str, domain: &str) -> anyhow::Result<AuthSettings> {
    parse(token).ok_or_else(|| anyhow!("malformed API key"))?;

    if ctx.special_api_keys.contains(token) {
        return Ok(AuthSettings {
            key: token.to_string(),
            user: Address::default(),
            authorized_subgraphs: vec![],
            budget_usd: None,
        });
    }

    // Note: holding watch::Ref for the rest of the function
    let api_keys = ctx.api_keys.borrow();

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
        user: api_key.user_address,
        authorized_subgraphs: api_key.subgraphs.clone(),
        budget_usd: api_key.max_budget_usd,
    })
}

fn parse(token: &str) -> Option<[u8; 16]> {
    if token.len() != 32 {
        return None;
    }
    let mut buf = [0_u8; 16];
    faster_hex::hex_decode(token.as_bytes(), &mut buf).ok()?;
    Some(buf)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::hex;

    use super::parse;

    #[test]
    fn parse_invalid_length_api_key() {
        assert_eq!(parse("0123456789abcdef0123456789abcde"), None);
    }

    #[test]
    fn parse_invalid_format_api_key() {
        assert_eq!(parse("abcdefghijklmnopqrstuvwxyz123456"), None);
    }

    #[test]
    fn parse_valid_api_key() {
        assert_eq!(
            parse("0123456789abcdef0123456789abcdef"),
            Some(hex!("0123456789abcdef0123456789abcdef"))
        );
    }
}
