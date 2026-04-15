//! PayAI facilitator authentication.
//!
//! Generates JWT tokens for authenticating with PayAI's x402 facilitator.
//! Uses EdDSA (Ed25519) algorithm as specified in PayAI documentation.

use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// JWT claims for PayAI authentication.
#[derive(Debug, Serialize)]
struct PayAiClaims {
    /// Subject - the API key ID
    sub: String,
    /// Issuer - always "payai-merchant"
    iss: String,
    /// Issued at time (Unix timestamp)
    iat: u64,
    /// Expiration time (Unix timestamp)
    exp: u64,
    /// Unique JWT ID for replay protection
    jti: String,
}

/// Generates a JWT for PayAI facilitator authentication.
///
/// The JWT is signed using EdDSA (Ed25519) algorithm with the API key secret.
/// The secret should be in base64-encoded PKCS#8/DER format, optionally with
/// a `payai_sk_` prefix that will be stripped.
pub fn generate_jwt(
    api_key_id: &str,
    api_key_secret: &str,
) -> Result<String, PayAiAuthError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    let claims = PayAiClaims {
        sub: api_key_id.to_string(),
        iss: "payai-merchant".to_string(),
        iat: now,
        exp: now + 120, // 2 minute expiration
        jti: Uuid::new_v4().to_string(),
    };

    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(api_key_id.to_string());

    // Strip optional prefix and decode base64
    let secret_clean = api_key_secret.trim_start_matches("payai_sk_");
    let secret_bytes = base64::engine::general_purpose::STANDARD
        .decode(secret_clean)
        .map_err(|e| PayAiAuthError::InvalidKey(format!("base64 decode failed: {e}")))?;

    let key = EncodingKey::from_ed_der(&secret_bytes);
    jsonwebtoken::encode(&header, &claims, &key).map_err(PayAiAuthError::Jwt)
}

/// Generates the Authorization header value for PayAI requests.
pub fn generate_auth_header(
    api_key_id: &str,
    api_key_secret: &str,
) -> Result<String, PayAiAuthError> {
    let jwt = generate_jwt(api_key_id, api_key_secret)?;
    Ok(format!("Bearer {}", jwt))
}

/// Errors that can occur during PayAI authentication.
#[derive(Debug, thiserror::Error)]
pub enum PayAiAuthError {
    #[error("Invalid API key: {0}")]
    InvalidKey(String),
    #[error("JWT encoding failed: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test Ed25519 key (for testing purposes only)
    // Generated with: openssl genpkey -algorithm Ed25519 -outform DER | base64
    const TEST_ED25519_KEY: &str = "MC4CAQAwBQYDK2VwBCIEIBkg4LVWM9nuwNFBQfJBrqHAXHPx0oQVaTdqDS7DD/Tn";

    #[test]
    fn test_generate_jwt() {
        let result = generate_jwt("test-key-id", TEST_ED25519_KEY);
        assert!(result.is_ok(), "JWT generation failed: {:?}", result.err());

        let jwt = result.unwrap();
        // JWT has 3 parts separated by dots
        assert_eq!(jwt.split('.').count(), 3);
    }

    #[test]
    fn test_generate_jwt_with_prefix() {
        let key_with_prefix = format!("payai_sk_{}", TEST_ED25519_KEY);
        let result = generate_jwt("test-key-id", &key_with_prefix);
        assert!(result.is_ok(), "JWT generation with prefix failed: {:?}", result.err());
    }

    #[test]
    fn test_generate_auth_header() {
        let result = generate_auth_header("test-key-id", TEST_ED25519_KEY);
        assert!(result.is_ok());
        assert!(result.unwrap().starts_with("Bearer "));
    }

    #[test]
    fn test_invalid_base64() {
        let result = generate_jwt("test-key-id", "not-valid-base64!!!");
        assert!(matches!(result, Err(PayAiAuthError::InvalidKey(_))));
    }
}
