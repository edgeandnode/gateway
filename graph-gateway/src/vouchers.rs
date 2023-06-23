use axum::{body::Bytes, extract::State, http::StatusCode};
use lazy_static::lazy_static;
use primitive_types::U256;
use secp256k1::{PublicKey, Secp256k1};
use serde::{Deserialize, Deserializer};
use serde_json::json;

use indexer_selection::{
    receipts::{self, combine_partial_vouchers, receipts_to_partial_voucher, receipts_to_voucher},
    SecretKey,
};
use prelude::*;

use crate::{json_response, metrics::*, JsonResponse};

lazy_static! {
    static ref SECP256K1: Secp256k1<secp256k1::All> = Secp256k1::new();
}

pub async fn handle_collect_receipts(
    State(signer): State<SecretKey>,
    payload: Bytes,
) -> Result<JsonResponse, (StatusCode, String)> {
    let _timer = METRICS.collect_receipts.duration.start_timer();
    match process_oneshot_voucher(&signer, &payload) {
        Ok(response) => {
            METRICS.collect_receipts.ok.inc();
            Ok(response)
        }
        Err(collect_receipts_err) => {
            METRICS.collect_receipts.err.inc();
            tracing::info!(%collect_receipts_err);
            Err((StatusCode::BAD_REQUEST, collect_receipts_err))
        }
    }
}

fn process_oneshot_voucher(signer: &SecretKey, payload: &Bytes) -> Result<JsonResponse, String> {
    let (allocation_id, receipts) = parse_receipts(payload)?;
    let allocation_signer = PublicKey::from_secret_key(&SECP256K1, signer);
    let voucher = receipts_to_voucher(&allocation_id, &allocation_signer, signer, receipts)
        .map_err(|err| err.to_string())?;
    tracing::info!(
        allocation = %Address(allocation_id),
        receipts_size = receipts.len(),
        fees = %voucher.fees.to_string(),
        "collect receipts request",
    );
    // Don't allow more than 10M GRT in a single collection
    if voucher.fees > U256::from(10000000000000000000000000u128) {
        tracing::error!(excessive_voucher_fees = %voucher.fees);
        return Err("Voucher value too large".into());
    }
    Ok(json_response(
        [],
        json!({
            "allocation": format!("0x{}", hex::encode(voucher.allocation_id)),
            "amount": voucher.fees.to_string(),
            "signature": format!("0x{}", hex::encode(voucher.signature)),
        }),
    ))
}

pub async fn handle_partial_voucher(
    State(signer): State<SecretKey>,
    payload: Bytes,
) -> Result<JsonResponse, (StatusCode, String)> {
    let _timer = METRICS.partial_voucher.duration.start_timer();
    match process_partial_voucher(&signer, &payload) {
        Ok(response) => {
            METRICS.partial_voucher.ok.inc();
            Ok(response)
        }
        Err(partial_voucher_err) => {
            METRICS.partial_voucher.err.inc();
            tracing::info!(%partial_voucher_err);
            Err((StatusCode::BAD_REQUEST, partial_voucher_err))
        }
    }
}

fn process_partial_voucher(signer: &SecretKey, payload: &Bytes) -> Result<JsonResponse, String> {
    let (allocation_id, receipts) = parse_receipts(payload)?;
    let allocation_signer = PublicKey::from_secret_key(&SECP256K1, signer);
    let partial_voucher =
        receipts_to_partial_voucher(&allocation_id, &allocation_signer, signer, receipts)
            .map_err(|err| err.to_string())?;
    tracing::info!(
        allocation = %Address(allocation_id),
        receipts_size = receipts.len(),
        fees = %partial_voucher.voucher.fees.to_string(),
        "partial voucher request",
    );
    // 10M GRT
    if partial_voucher.voucher.fees > U256::from(10000000000000000000000000u128) {
        tracing::error!(excessive_voucher_fees = %partial_voucher.voucher.fees);
        return Err("Voucher value too large".into());
    }
    Ok(json_response(
        [],
        json!({
            "allocation": format!("0x{}", hex::encode(partial_voucher.voucher.allocation_id)),
            "fees": partial_voucher.voucher.fees.to_string(),
            "signature": format!("0x{}", hex::encode(partial_voucher.voucher.signature)),
            "receipt_id_min": format!("0x{}", hex::encode(partial_voucher.receipt_id_min)),
            "receipt_id_max": format!("0x{}", hex::encode(partial_voucher.receipt_id_max)),
        }),
    ))
}

pub async fn handle_voucher(
    State(signer): State<SecretKey>,
    payload: Bytes,
) -> Result<JsonResponse, (StatusCode, String)> {
    let _timer = METRICS.voucher.duration.start_timer();
    match process_voucher(&signer, &payload) {
        Ok(response) => {
            METRICS.voucher.ok.inc();
            Ok(response)
        }
        Err(voucher_err) => {
            METRICS.voucher.err.inc();
            tracing::info!(%voucher_err);
            Err((StatusCode::BAD_REQUEST, voucher_err))
        }
    }
}

fn process_voucher(signer: &SecretKey, payload: &Bytes) -> Result<JsonResponse, String> {
    let request =
        serde_json::from_slice::<VoucherRequest>(payload).map_err(|err| err.to_string())?;
    let allocation_id = request.allocation;
    let partial_vouchers = request
        .partial_vouchers
        .into_iter()
        .map(|pv| receipts::PartialVoucher {
            voucher: receipts::Voucher {
                allocation_id: *allocation_id,
                fees: pv.fees,
                signature: pv.signature,
            },
            receipt_id_min: *pv.receipt_id_min,
            receipt_id_max: *pv.receipt_id_max,
        })
        .collect::<Vec<receipts::PartialVoucher>>();
    let voucher = combine_partial_vouchers(&allocation_id, signer, &partial_vouchers)
        .map_err(|err| err.to_string())?;
    tracing::info!(
        allocation = %allocation_id,
        partial_vouchers = partial_vouchers.len(),
        fees = %voucher.fees.to_string(),
        "voucher request",
    );
    // 10M GRT
    if voucher.fees > U256::from(10000000000000000000000000u128) {
        tracing::error!(excessive_voucher_fees = %voucher.fees);
        return Err("Voucher value too large".into());
    }
    Ok(json_response(
        [],
        json!({
            "allocation": allocation_id.to_string(),
            "fees": voucher.fees.to_string(),
            "signature": format!("0x{}", hex::encode(voucher.signature)),
        }),
    ))
}

fn parse_receipts(payload: &[u8]) -> Result<([u8; 20], &[u8]), String> {
    if payload.len() < 20 {
        return Err("Invalid request data".into());
    }
    let mut allocation_id = [0u8; 20];
    allocation_id.copy_from_slice(&payload[..20]);
    Ok((allocation_id, &payload[20..]))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VoucherRequest {
    allocation: Address,
    partial_vouchers: Vec<PartialVoucher>,
}

#[derive(Deserialize)]
struct PartialVoucher {
    #[serde(deserialize_with = "deserialize_signature")]
    signature: [u8; 65],
    #[serde(deserialize_with = "deserialize_u256")]
    fees: U256,
    receipt_id_min: ReceiptID,
    receipt_id_max: ReceiptID,
}

bytes_wrapper!(pub, ReceiptID, 15, "HexStr");

fn deserialize_signature<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 65], D::Error> {
    let input: &str = Deserialize::deserialize(deserializer)?;
    let mut signature = [0u8; 65];
    let offset = if input.starts_with("0x") { 2 } else { 0 };
    hex::decode_to_slice(&input[offset..], &mut signature).map_err(serde::de::Error::custom)?;
    Ok(signature)
}

fn deserialize_u256<'de, D: Deserializer<'de>>(deserializer: D) -> Result<U256, D::Error> {
    let input: &str = Deserialize::deserialize(deserializer)?;
    // U256::from_str is busted, so use the equivalent decimals representation
    Ok(GRTWei::from_str(input)
        .map_err(serde::de::Error::custom)?
        .as_u256())
}
