//! Query Handler Context
//!
//! Shared context passed to all query handlers via Axum state.
//!
//! # Lifetime Requirements
//!
//! Several fields use `&'static` references because Axum's state must be
//! `Clone + Send + Sync + 'static`. These are singletons initialized once
//! at startup and never deallocated (via `Box::leak`).
//!
//! See [`main`](crate::main) module documentation for rationale.

use ordered_float::NotNan;
use thegraph_core::alloy::dyn_abi::Eip712Domain;
use tokio::sync::{mpsc, watch};

use crate::{
    budgets::Budgeter, chains::Chains, indexer_client::IndexerClient,
    indexing_performance::IndexingPerformance, network::NetworkService, receipts::ReceiptSigner,
    reports,
};

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub receipt_signer: &'static ReceiptSigner,
    pub budgeter: &'static Budgeter,
    pub grt_per_usd: watch::Receiver<NotNan<f64>>,
    pub chains: &'static Chains,
    pub network: NetworkService,
    pub indexing_perf: IndexingPerformance,
    pub attestation_domain: &'static Eip712Domain,
    pub legacy_attestation_domain: &'static Eip712Domain,
    pub reporter: mpsc::UnboundedSender<reports::ClientRequest>,
}
