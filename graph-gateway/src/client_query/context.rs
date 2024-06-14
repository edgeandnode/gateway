use alloy_sol_types::Eip712Domain;
use gateway_framework::{budgets::Budgeter, chains::Chains};
use ordered_float::NotNan;
use tokio::sync::{mpsc, watch};
use url::Url;

use crate::{
    indexer_client::IndexerClient, indexing_performance::IndexingPerformance,
    network::NetworkService, receipts::ReceiptSigner, reports,
};

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub receipt_signer: &'static ReceiptSigner,
    pub budgeter: &'static Budgeter,
    pub l2_gateway: Option<Url>,
    pub grt_per_usd: watch::Receiver<NotNan<f64>>,
    pub chains: &'static Chains,
    pub network: NetworkService,
    pub indexing_perf: IndexingPerformance,
    pub attestation_domain: &'static Eip712Domain,
    pub reporter: mpsc::UnboundedSender<reports::ClientRequest>,
}
