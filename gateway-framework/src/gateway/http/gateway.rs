use alloy_primitives::BlockNumber;
use axum::http::HeaderMap;
use thegraph_core::types::Attestation;

use crate::blocks::Block;

pub trait IndexingStatus: Send + Sync + 'static {
    fn block(&self) -> BlockNumber;
    fn min_block(&self) -> Option<BlockNumber>;
    fn legacy_scalar(&self) -> bool;
}

#[derive(Clone, Debug)]
pub struct DeterministicRequest {
    pub body: String,
    pub headers: HeaderMap,
}

#[derive(Clone)]
pub struct IndexerResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub attestation: Option<Attestation>,
    pub original_response: String,
    pub client_response: String,
    pub errors: Vec<String>,
    pub probe_block: Option<Block>,
}
