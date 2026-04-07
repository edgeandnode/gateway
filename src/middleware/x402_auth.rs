//! x402 payment authentication middleware.
//!
//! This middleware handles x402 payment verification and injects `AuthSettings`
//! for downstream handlers.

use axum::{body::Body, extract::Request, http::Response, middleware::Next};
use base64::Engine;
use ordered_float::NotNan;
use x402_axum::{facilitator_client::FacilitatorClient, StaticPriceTags, X402LayerBuilder, X402Middleware};
use x402_chain_eip155::{
    KnownNetworkEip155, V2Eip155Exact,
    v2_eip155_exact::types::{ExactEvmPayload, PaymentPayload},
};
use x402_types::{networks::USDC, proto::v2::PriceTag};

use crate::{auth::AuthSettings, config::{X402Chain, X402Config}};

/// Creates middleware that manages the x402 payment flow.
pub fn create_layer(
    config: &X402Config,
    query_fees_target: NotNan<f64>,
) -> X402LayerBuilder<StaticPriceTags<PriceTag>, std::sync::Arc<FacilitatorClient>> {
    let usdc_amount = (*query_fees_target * 1_000_000.0) as u64;
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
