use crate::{
    indexer_client::IndexerClient, indexers::indexing, indexing_performance::IndexingPerformance,
    reports::KafkaClient, topology::GraphNetwork,
};
use alloy_primitives::Address;
use alloy_sol_types::Eip712Domain;
use eventuals::{Eventual, Ptr};
use gateway_common::types::Indexing;
use gateway_framework::{budgets::Budgeter, scalar::ReceiptSigner};
use ordered_float::NotNan;
use std::collections::{HashMap, HashSet};
use url::Url;

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub receipt_signer: &'static ReceiptSigner,
    pub kafka_client: &'static KafkaClient,
    pub budgeter: &'static Budgeter,
    pub indexer_selection_retry_limit: usize,
    pub l2_gateway: Option<Url>,
    pub grt_per_usd: Eventual<NotNan<f64>>,
    pub network: GraphNetwork,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, indexing::Status>>>,
    pub indexing_perf: IndexingPerformance,
    pub attestation_domain: &'static Eip712Domain,
    pub bad_indexers: &'static HashSet<Address>,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
}
