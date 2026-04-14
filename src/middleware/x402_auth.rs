//! x402 payment middleware
//!
//! Middleware consists of two layers:
//! - x402 middleware, handling base x402 payments
//! - x402 auth adapter, extracting payer info and injecting `AuthSettings` request extension for downstream handlers.

use axum::{body::Body, extract::Request, http::Response, middleware::Next};
use base64::Engine;
use x402_axum::{
    StaticPriceTags, X402LayerBuilder, X402Middleware, facilitator_client::FacilitatorClient,
};
use x402_chain_eip155::{
    KnownNetworkEip155, V2Eip155Exact,
    v2_eip155_exact::types::{ExactEvmPayload, PaymentPayload},
};
use x402_types::{networks::USDC, proto::v2::PriceTag};

use crate::{
    auth::AuthSettings,
    config::{X402Chain, X402Config},
};

/// Creates middleware that manages the x402 payment flow.
pub fn create_layer(
    config: &X402Config,
) -> X402LayerBuilder<StaticPriceTags<PriceTag>, std::sync::Arc<FacilitatorClient>> {
    let usdc_amount = (*config.price * 1_000_000.0) as u64;
    let token = match config.chain {
        X402Chain::Base => USDC::base(),
        X402Chain::BaseSepolia => USDC::base_sepolia(),
    };
    let price_tag = V2Eip155Exact::price_tag(config.receiver_address, token.amount(usdc_amount));
    X402Middleware::new(config.facilitator_url.as_str()).with_price_tag(price_tag)
}

/// Extracts payer address from x402 payment header and inserts AuthSettings.
/// This adapter middleware extracts the payer address from the x402 payment header,
/// and adds it to the request as an `AuthSettings` extension.
///
/// If the request already has an `AuthSettings` extension, it does nothing.
/// If the payer address cannot be extracted from the x402 payment header, it defaults to "x402".
pub async fn x402_auth_adapter(mut request: Request, next: Next) -> Response<Body> {
    if request.extensions().get::<AuthSettings>().is_some() {
        return next.run(request).await;
    }

    let payer = extract_payer_address(request.headers()).unwrap_or_else(|| "x402".into());

    request.extensions_mut().insert(AuthSettings {
        key: "x402".into(),
        user: payer,
        authorized_subgraphs: vec![],
    });

    next.run(request).await
}

/// Decode payment header to extract payer address using x402 types.
fn extract_payer_address(headers: &axum::http::HeaderMap) -> Option<String> {
    let header = headers.get("x-payment")?;

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(header.as_bytes())
        .ok()?;

    let payment: PaymentPayload = serde_json::from_slice(&decoded).ok()?;

    let address = match payment.payload {
        ExactEvmPayload::Eip3009(p) => format!("{}", p.authorization.from),
        ExactEvmPayload::Permit2(p) => p.permit_2_authorization.from.to_string(),
    };

    Some(address)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;
    use base64::Engine;

    use super::extract_payer_address;

    #[test]
    fn extract_payer_missing_header() {
        let headers = HeaderMap::new();
        assert_eq!(extract_payer_address(&headers), None);
    }

    #[test]
    fn extract_payer_invalid_base64() {
        let mut headers = HeaderMap::new();
        headers.insert("x-payment", "not-valid-base64!!!".parse().unwrap());
        assert_eq!(extract_payer_address(&headers), None);
    }

    #[test]
    fn extract_payer_invalid_json() {
        let mut headers = HeaderMap::new();
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"not json");
        headers.insert("x-payment", encoded.parse().unwrap());
        assert_eq!(extract_payer_address(&headers), None);
    }

    #[test]
    fn extract_payer_valid_eip3009_payload() {
        // Minimal valid Eip3009 payment payload
        let payload = serde_json::json!({
            "x402Version": 2,
            "accepted": {
                "scheme": "exact",
                "network": "eip155:8453",
                "amount": "100",
                "payTo": "0x0000000000000000000000000000000000000001",
                "maxTimeoutSeconds": 300,
                "asset": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "extra": {
                    "assetTransferMethod": "eip3009",
                    "name": "USDC",
                    "version": "2"
                }
            },
            "payload": {
                "signature": "0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                "authorization": {
                    "from": "0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF",
                    "to": "0x0000000000000000000000000000000000000001",
                    "value": "100",
                    "validAfter": "0",
                    "validBefore": "1999999999",
                    "nonce": "0x0000000000000000000000000000000000000000000000000000000000000001"
                }
            },
            "resource": null
        });

        let mut headers = HeaderMap::new();
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(payload.to_string().as_bytes());
        headers.insert("x-payment", encoded.parse().unwrap());

        let result = extract_payer_address(&headers);
        assert_eq!(
            result,
            Some("0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF".to_string())
        );
    }
}
