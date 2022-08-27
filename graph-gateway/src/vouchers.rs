use actix_web::{http::StatusCode, web, HttpResponse, HttpResponseBuilder};
use hex;
use indexer_selection::SecretKey;
use lazy_static::lazy_static;
use prelude::*;
use primitive_types::U256;
use receipts::{self, combine_partial_vouchers, receipts_to_partial_voucher, receipts_to_voucher};
use secp256k1::{PublicKey, Secp256k1};
use serde::{Deserialize, Deserializer};
use serde_json::json;

lazy_static! {
    static ref SECP256K1: Secp256k1<secp256k1::All> = Secp256k1::new();
}

#[tracing::instrument(skip(data, payload))]
pub async fn handle_collect_receipts(
    data: web::Data<SecretKey>,
    payload: web::Bytes,
) -> HttpResponse {
    let _timer = METRICS.collect_receipts.duration.start_timer();
    match process_oneshot_voucher(&data, &payload) {
        Ok(response) => {
            METRICS.collect_receipts.ok.inc();
            response
        }
        Err(collect_receipts_err) => {
            METRICS.collect_receipts.failed.inc();
            tracing::info!(%collect_receipts_err);
            HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(collect_receipts_err)
        }
    }
}

fn process_oneshot_voucher(
    signer: &SecretKey,
    payload: &web::Bytes,
) -> Result<HttpResponse, String> {
    let (allocation_id, receipts) = parse_receipts(payload)?;
    let allocation_signer = PublicKey::from_secret_key(&SECP256K1, signer);
    let voucher = receipts_to_voucher(&allocation_id, &allocation_signer, signer, &receipts)
        .map_err(|err| err.to_string())?;
    tracing::info!(
        allocation = %Address(allocation_id),
        receipts_size = receipts.len(),
        fees = %voucher.fees.to_string(),
        "collect receipts request",
    );
    Ok(HttpResponseBuilder::new(StatusCode::OK).json(json!({
        "allocation": format!("0x{}", hex::encode(voucher.allocation_id)),
        "amount": voucher.fees.to_string(),
        "signature": format!("0x{}", hex::encode(voucher.signature)),
    })))
}

#[tracing::instrument(skip(data, payload))]
pub async fn handle_partial_voucher(
    data: web::Data<SecretKey>,
    payload: web::Bytes,
) -> HttpResponse {
    let _timer = METRICS.partial_voucher.duration.start_timer();
    match process_partial_voucher(&data, &payload) {
        Ok(response) => {
            METRICS.partial_voucher.ok.inc();
            response
        }
        Err(partial_voucher_err) => {
            METRICS.partial_voucher.failed.inc();
            tracing::info!(%partial_voucher_err);
            HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(partial_voucher_err)
        }
    }
}

fn process_partial_voucher(
    signer: &SecretKey,
    payload: &web::Bytes,
) -> Result<HttpResponse, String> {
    let (allocation_id, receipts) = parse_receipts(&payload)?;
    let allocation_signer = PublicKey::from_secret_key(&SECP256K1, signer);
    let partial_voucher =
        receipts_to_partial_voucher(&allocation_id, &allocation_signer, signer, &receipts)
            .map_err(|err| err.to_string())?;
    tracing::info!(
        allocation = %Address(allocation_id),
        receipts_size = receipts.len(),
        fees = %partial_voucher.voucher.fees.to_string(),
        "partial voucher request",
    );
    Ok(HttpResponseBuilder::new(StatusCode::OK).json(json!({
        "allocation": format!("0x{}", hex::encode(partial_voucher.voucher.allocation_id)),
        "fees": partial_voucher.voucher.fees.to_string(),
        "signature": format!("0x{}", hex::encode(partial_voucher.voucher.signature)),
        "receipt_id_min": format!("0x{}", hex::encode(partial_voucher.receipt_id_min)),
        "receipt_id_max": format!("0x{}", hex::encode(partial_voucher.receipt_id_max)),
    })))
}

#[tracing::instrument(skip(data, payload))]
pub async fn handle_voucher(data: web::Data<SecretKey>, payload: web::Bytes) -> HttpResponse {
    let _timer = METRICS.voucher.duration.start_timer();
    match process_voucher(&data, &payload) {
        Ok(response) => {
            METRICS.voucher.ok.inc();
            response
        }
        Err(voucher_err) => {
            METRICS.voucher.failed.inc();
            tracing::info!(%voucher_err);
            HttpResponseBuilder::new(StatusCode::BAD_REQUEST).body(voucher_err)
        }
    }
}

fn process_voucher(signer: &SecretKey, payload: &web::Bytes) -> Result<HttpResponse, String> {
    let request =
        serde_json::from_slice::<VoucherRequest>(&payload).map_err(|err| err.to_string())?;
    let allocation_id = request.allocation_id;
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
    Ok(HttpResponseBuilder::new(StatusCode::OK).json(json!({
        "allocation": allocation_id.to_string(),
        "fees": voucher.fees.to_string(),
        "signature": format!("0x{}", hex::encode(voucher.signature)),
    })))
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
    allocation_id: Address,
    partial_vouchers: Vec<PartialVoucher>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
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
    U256::from_str(input).map_err(serde::de::Error::custom)
}

#[derive(Clone)]
struct Metrics {
    collect_receipts: ResponseMetrics,
    partial_voucher: ResponseMetrics,
    voucher: ResponseMetrics,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
}

impl Metrics {
    fn new() -> Self {
        Self {
            collect_receipts: ResponseMetrics::new(
                "gateway_collect_receipts",
                "requests to collect-receipts",
            ),
            partial_voucher: ResponseMetrics::new(
                "gateway_partial_voucher",
                "requests for partial-voucher",
            ),
            voucher: ResponseMetrics::new("gateway_voucher", "requests for voucher"),
        }
    }
}
