use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::FromRef;
use eventuals::{Eventual, Ptr};

use crate::subgraph_studio::APIKey;

use super::AuthHandler;

/// Errors that may occur when parsing a Studio API key.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid length: {0}")]
    InvalidLength(usize),

    #[error("invalid format: {0}")]
    InvalidHex(faster_hex::Error),
}

/// Parses a Studio API key from a string.
///
/// The API key is expected to be a 32-character hex string.
pub fn parse_studio_api_key(value: &str) -> Result<[u8; 16], ParseError> {
    if value.len() != 32 {
        return Err(ParseError::InvalidLength(value.len()));
    }

    let mut buf = [0_u8; 16];
    faster_hex::hex_decode(value.as_bytes(), &mut buf).map_err(ParseError::InvalidHex)?;
    Ok(buf)
}

/// App state (a.k.a [Context](crate::client_query::Context)) sub-state.
pub struct StudioAuthHandler {
    /// A map between Studio auth bearer token string and the Studio [ApiKey](crate::subgraph_studio::APIKey).
    ///
    /// API keys are fetched periodically (every 30s) from the Studio API by the gateway using the
    /// [`subgraph_studio` client](crate::subgraph_studio::Client).
    studio_keys: Eventual<Ptr<HashMap<String, Arc<APIKey>>>>,
}

// TODO(LNSD): Use `client_query::Context` instead of `AuthHandler`.
impl FromRef<AuthHandler> for StudioAuthHandler {
    fn from_ref(auth: &AuthHandler) -> Self {
        Self {
            studio_keys: auth.api_keys.clone(),
        }
    }
}

impl StudioAuthHandler {
    /// Get the Studio API key associated with the given bearer token string.
    pub fn get_api_key(&self, token: &str) -> Option<Arc<APIKey>> {
        self.studio_keys.value_immediate()?.get(token).cloned()
    }
}

pub fn parse_bearer_token<S>(state: &S, token: &str) -> anyhow::Result<Arc<APIKey>>
where
    StudioAuthHandler: FromRef<S>,
{
    // Check if the bearer token is a valid 32 hex digits key
    if parse_studio_api_key(token).is_err() {
        return Err(anyhow::anyhow!("Invalid api key format"));
    }

    // Retrieve the API Key associated with the bearer token
    let auth_handler = StudioAuthHandler::from_ref(state);
    auth_handler
        .get_api_key(token)
        .ok_or_else(|| anyhow::anyhow!("API key not found"))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod parser {
        use assert_matches::assert_matches;

        use super::{parse_studio_api_key, ParseError};

        #[test]
        fn parse_invalid_length_studio_api_key() {
            //* Given
            let api_key = "0123456789abcdef0123456789abcde";

            //* When
            let result = parse_studio_api_key(api_key);

            //* Then
            assert_matches!(result, Err(ParseError::InvalidLength(31)));
        }

        #[test]
        fn parse_invalid_format_studio_api_key() {
            //* Given
            let api_key = "abcdefghijklmnopqrstuvwxyz123456";

            //* When
            let result = parse_studio_api_key(api_key);

            //* Then
            assert_matches!(result, Err(ParseError::InvalidHex(_)));
        }

        #[test]
        fn parse_valid_studio_api_key() {
            //* Given
            let api_key = "0123456789abcdef0123456789abcdef";

            //* When
            let result = parse_studio_api_key(api_key);

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
