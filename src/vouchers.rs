use axum::{body::Bytes, extract::State, http::StatusCode};
use lazy_static::lazy_static;
use receipts::{self, combine_partial_vouchers, receipts_to_partial_voucher, receipts_to_voucher};
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use serde::Deserialize;
use serde_json::json;
use thegraph_core::alloy::{
    hex,
    primitives::{Address, FixedBytes, U256},
};

use crate::{
    json::{json_response, JsonResponse},
    metrics::METRICS,
};

lazy_static! {
    static ref SECP256K1: Secp256k1<secp256k1::All> = Secp256k1::new();
}

pub async fn handle_collect_receipts(
    State(signer): State<&'static SecretKey>,
    payload: Bytes,
) -> Result<JsonResponse, (StatusCode, String)> {
    let _timer = METRICS.collect_receipts.duration.start_timer();
    match process_oneshot_voucher(signer, &payload) {
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
        allocation = %Address::from(allocation_id),
        receipts_size = receipts.len(),
        fees = %voucher.fees.to_string(),
        "collect receipts request",
    );
    // Don't allow more than 10M GRT in a single collection
    if voucher.fees > primitive_types::U256::from(10000000000000000000000000_u128) {
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
    State(signer): State<&'static SecretKey>,
    payload: Bytes,
) -> Result<JsonResponse, (StatusCode, String)> {
    let _timer = METRICS.partial_voucher.duration.start_timer();
    match process_partial_voucher(signer, &payload) {
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
        allocation = %Address::from(allocation_id),
        receipts_size = receipts.len(),
        fees = %partial_voucher.voucher.fees.to_string(),
        "partial voucher request",
    );
    // 10M GRT
    if partial_voucher.voucher.fees > primitive_types::U256::from(10000000000000000000000000u128) {
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
    State(signer): State<&'static SecretKey>,
    payload: Bytes,
) -> Result<JsonResponse, (StatusCode, String)> {
    let _timer = METRICS.voucher.duration.start_timer();
    match process_voucher(signer, &payload) {
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
                allocation_id: allocation_id.into(),
                fees: primitive_types::U256::from_little_endian(&pv.fees.as_le_bytes()),
                signature: pv.signature.into(),
            },
            receipt_id_min: *pv.receipt_id_min,
            receipt_id_max: *pv.receipt_id_max,
        })
        .collect::<Vec<receipts::PartialVoucher>>();
    let voucher = combine_partial_vouchers(&allocation_id.0, signer, &partial_vouchers)
        .map_err(|err| err.to_string())?;
    tracing::info!(
        allocation = %allocation_id,
        partial_vouchers = partial_vouchers.len(),
        fees = %voucher.fees.to_string(),
        "voucher request",
    );
    // 10M GRT
    if voucher.fees > primitive_types::U256::from(10000000000000000000000000u128) {
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
    signature: Signature,
    fees: U256,
    receipt_id_min: ReceiptID,
    receipt_id_max: ReceiptID,
}

type Signature = FixedBytes<65>;
type ReceiptID = FixedBytes<15>;
