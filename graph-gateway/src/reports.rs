use alloy_primitives::Address;
use gateway_common::utils::timestamp::unix_timestamp;
use gateway_framework::{
    errors::IndexerError,
    reporting::{error_log, KafkaClient, CLIENT_REQUEST_TARGET, INDEXER_REQUEST_TARGET},
};
use prost::Message as _;
use serde::Deserialize;
use serde_json::{json, Map};
use thegraph_core::types::attestation::Attestation;
use toolshed::concat_bytes;

pub fn report_client_query(kafka: &KafkaClient, fields: Map<String, serde_json::Value>) {
    #[derive(Deserialize)]
    struct Fields {
        request_id: String,
        graph_env: String,
        legacy_status_message: String,
        legacy_status_code: u32,
        start_time_ms: u64,
        deployment: Option<String>,
        user_address: Option<String>,
        api_key: Option<String>,
        subgraph_chain: Option<String>,
        query_count: Option<u32>,
        budget_grt: Option<f32>,
        indexer_fees_grt: Option<f32>,
        indexer_fees_usd: Option<f32>,
    }
    let fields = match serde_json::from_value::<Fields>(fields.into()) {
        Ok(fields) => fields,
        Err(err) => {
            error_log(
                CLIENT_REQUEST_TARGET,
                &format!("failed to report client query: {}", err),
            );
            return;
        }
    };

    let timestamp = unix_timestamp();
    let response_time_ms = timestamp.saturating_sub(fields.start_time_ms) as u32;

    // data science: bigquery datasets still rely on this log line
    let log = serde_json::to_string(&json!({
        "target": CLIENT_REQUEST_TARGET,
        "level": "INFO",
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        "fields": {
            "message": "Client query result",
            "query_id": &fields.request_id,
            "ray_id": &fields.request_id, // In production this will be the Ray ID.
            "deployment": fields.deployment.as_deref().unwrap_or(""),
            "network": fields.subgraph_chain.as_deref().unwrap_or(""),
            "user": &fields.user_address,
            "api_key": &fields.api_key,
            "query_count": fields.query_count.unwrap_or(0),
            "budget": fields.budget_grt.unwrap_or(0.0).to_string(),
            "fee": fields.indexer_fees_grt.unwrap_or(0.0),
            "fee_usd": fields.indexer_fees_usd.unwrap_or(0.0),
            "response_time_ms": response_time_ms,
            "status": &fields.legacy_status_message,
            "status_code": fields.legacy_status_code,
        },
    }))
    .unwrap();
    println!("{log}");

    let kafka_msg = json!({
        "query_id": &fields.request_id,
        "ray_id": &fields.request_id, // In production this will be the Ray ID.
        "graph_env": &fields.graph_env,
        "timestamp": timestamp,
        "user": &fields.user_address,
        "api_key": &fields.api_key,
        "deployment": &fields.deployment.as_deref().unwrap_or(""),
        "network": &fields.subgraph_chain.as_deref().unwrap_or(""),
        "response_time_ms": response_time_ms,
        "budget": fields.budget_grt.unwrap_or(0.0).to_string(),
        "budget_float": fields.budget_grt,
        "query_count": fields.query_count.unwrap_or(0),
        "fee": fields.indexer_fees_grt.unwrap_or(0.0),
        "fee_usd": fields.indexer_fees_usd.unwrap_or(0.0),
        "status": &fields.legacy_status_message,
        "status_code": fields.legacy_status_code,
    });
    kafka.send(
        "gateway_client_query_results",
        &serde_json::to_vec(&kafka_msg).unwrap(),
    );
}

pub fn report_indexer_query(kafka: &KafkaClient, fields: Map<String, serde_json::Value>) {
    #[derive(Deserialize)]
    struct Fields {
        request_id: String,
        graph_env: String,
        api_key: Option<String>,
        user_address: Option<String>,
        status_message: String,
        status_code: u32,
        response_time_ms: u32,
        deployment: String,
        subgraph_chain: String,
        indexer: String,
        url: String,
        blocks_behind: u64,
        fee_grt: f32,
        legacy_scalar: Option<bool>,
        allocation: Option<String>,
        indexer_errors: Option<String>,
    }
    let fields = match serde_json::from_value::<Fields>(fields.into()) {
        Ok(fields) => fields,
        Err(err) => {
            error_log(
                INDEXER_REQUEST_TARGET,
                &format!("failed to report indexer query: {}", err),
            );
            return;
        }
    };

    // data science: bigquery datasets still rely on this log line
    let log = serde_json::to_string(&json!({
        "target": INDEXER_REQUEST_TARGET,
        "level": "INFO",
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        "fields": {
            "message": "Indexer attempt",
            "query_id": &fields.request_id,
            "ray_id": &fields.request_id, // In production this will be the Ray ID.
            "deployment": &fields.deployment,
            "indexer": &fields.indexer,
            "url": &fields.url,
            "blocks_behind": fields.blocks_behind,
            "attempt_index": 0,
            "api_key": fields.api_key.as_deref().unwrap_or(""),
            "fee": fields.fee_grt,
            "response_time_ms": fields.response_time_ms,
            "allocation": &fields.allocation,
            "indexer_errors": &fields.indexer_errors,
            "status": &fields.status_message,
            "status_code": fields.status_code,
        },
    }))
    .unwrap();
    println!("{log}");

    let kafka_msg = json!({
        "query_id": &fields.request_id,
        "ray_id": &fields.request_id, // In production this will be the Ray ID.
        "graph_env": &fields.graph_env,
        "timestamp": unix_timestamp(),
        "api_key": fields.api_key.as_deref().unwrap_or(""),
        "user_address": fields.user_address.as_deref().unwrap_or(""),
        "deployment": &fields.deployment,
        "network": &fields.subgraph_chain,
        "indexer": &fields.indexer,
        "url": &fields.url,
        "fee": fields.fee_grt,
        "legacy_scalar": fields.legacy_scalar.unwrap_or(false),
        "utility": 1.0,
        "blocks_behind": fields.blocks_behind,
        "response_time_ms": fields.response_time_ms,
        "allocation": fields.allocation.as_deref().unwrap_or(""),
        "indexer_errors": fields.indexer_errors.as_deref().unwrap_or(""),
        "status": &fields.status_message,
        "status_code": fields.status_code,
    });
    kafka.send(
        "gateway_indexer_attempts",
        &serde_json::to_vec(&kafka_msg).unwrap(),
    );
}

// Like much of this file. This is maintained is a partially backward-compatible state for data
// science, and should be deleted ASAP.
pub fn indexer_error_status_code(error: Option<&IndexerError>) -> u32 {
    let (prefix, data) = match error {
        None => (0x0, 200_u32.to_be()),
        Some(IndexerError::Internal(_)) => (0x1, 0x0),
        Some(IndexerError::Unavailable(_)) => (0x2, 0x0),
        Some(IndexerError::Timeout) => (0x3, 0x0),
        Some(IndexerError::BadResponse(_)) => (0x4, 0x0),
    };
    (prefix << 28) | (data & (u32::MAX >> 4))
}

pub fn serialize_attestation(
    attestation: &Attestation,
    allocation: Address,
    request: String,
    response: String,
) -> Vec<u8> {
    // Limit string payloads to 10 KB.
    const MAX_LEN: usize = 10_000;
    AttestationProtobuf {
        request: (request.len() <= MAX_LEN).then_some(request),
        response: (response.len() <= MAX_LEN).then_some(response),
        allocation: allocation.0 .0.into(),
        subgraph_deployment: attestation.deployment.0.into(),
        request_cid: attestation.request_cid.0.into(),
        response_cid: attestation.response_cid.0.into(),
        signature: concat_bytes!(65, [&[attestation.v], &attestation.r.0, &attestation.s.0]).into(),
    }
    .encode_to_vec()
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct AttestationProtobuf {
    #[prost(string, optional, tag = "1")]
    request: Option<String>,
    #[prost(string, optional, tag = "2")]
    response: Option<String>,
    /// 20 bytes
    #[prost(bytes, tag = "3")]
    allocation: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "4")]
    subgraph_deployment: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "5")]
    request_cid: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "6")]
    response_cid: Vec<u8>,
    /// 65 bytes, ECDSA signature (v, r, s)
    #[prost(bytes, tag = "7")]
    signature: Vec<u8>,
}
