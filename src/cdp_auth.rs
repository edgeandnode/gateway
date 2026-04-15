//! Coinbase Developer Platform (CDP) authentication.
//!
//! Generates JWT tokens for authenticating with CDP's x402 facilitator.

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

/// JWT claims for CDP authentication.
#[derive(Debug, Serialize)]
struct CdpClaims {
    /// Subject - the API key ID
    sub: String,
    /// Issuer - always "cdp"
    iss: String,
    /// Expiration time (Unix timestamp)
    exp: u64,
    /// Not before time (Unix timestamp)
    nbf: u64,
    /// The request URI
    uri: String,
}

/// Generates a JWT for CDP facilitator authentication.
///
/// The JWT is signed using ES256 algorithm with the API key secret.
/// The secret should be in PEM format (EC private key).
pub fn generate_jwt(
    api_key_id: &str,
    api_key_secret: &str,
    uri: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    let claims = CdpClaims {
        sub: api_key_id.to_string(),
        iss: "cdp".to_string(),
        nbf: now,
        exp: now + 120, // 2 minute expiration
        uri: uri.to_string(),
    };

    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(api_key_id.to_string());

    let key = EncodingKey::from_ec_pem(api_key_secret.as_bytes())?;
    jsonwebtoken::encode(&header, &claims, &key)
}

/// Generates the Authorization header value for CDP requests.
pub fn generate_auth_header(
    api_key_id: &str,
    api_key_secret: &str,
    uri: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    let jwt = generate_jwt(api_key_id, api_key_secret, uri)?;
    Ok(format!("Bearer {}", jwt))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test EC P-256 key for testing purposes only (ES256 requires P-256/prime256v1 curve)
    // Generated with: openssl ecparam -genkey -name prime256v1 -noout | openssl pkcs8 -topk8 -nocrypt
    const TEST_EC_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgUfXj0RLcZpbgZ3oM
kuPB/VJI0w6sTq4cB8YIgIJzoH+hRANCAASkrfCuhhoOXemBgZ+7ZUiOXwrrwozX
wctBS50Ra80xt49TtvaGt4H/UNjAemFY5R6L57Tfc8hRRWBwXtYCrQmI
-----END PRIVATE KEY-----"#;

    #[test]
    fn test_generate_jwt() {
        let result = generate_jwt("test-key-id", TEST_EC_KEY, "https://example.com/verify");
        assert!(result.is_ok(), "JWT generation failed: {:?}", result.err());

        let jwt = result.unwrap();
        // JWT has 3 parts separated by dots
        assert_eq!(jwt.split('.').count(), 3);
    }

    #[test]
    fn test_generate_auth_header() {
        let result =
            generate_auth_header("test-key-id", TEST_EC_KEY, "https://example.com/verify");
        assert!(result.is_ok(), "Auth header generation failed: {:?}", result.err());
        assert!(result.unwrap().starts_with("Bearer "));
    }
}
