use alloy_primitives::Address;
use alloy_sol_types::Eip712Domain;
use eventuals::{Eventual, Ptr};
use ordered_float::NotNan;
use std::collections::{HashMap, HashSet};
use url::Url;

use gateway_common::types::Indexing;
use gateway_framework::{
    budgets::Budgeter, chains::Chains, network::discovery::Status, reporting::KafkaClient,
    scalar::ReceiptSigner, topology::network::GraphNetwork,
};

use crate::{indexer_client::IndexerClient, indexing_performance::IndexingPerformance};

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub receipt_signer: &'static ReceiptSigner,
    pub kafka_client: &'static KafkaClient,
    pub budgeter: &'static Budgeter,
    pub indexer_selection_retry_limit: usize,
    pub l2_gateway: Option<Url>,
    pub grt_per_usd: Eventual<NotNan<f64>>,
    pub chains: &'static Chains,
    pub network: GraphNetwork,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    pub indexing_perf: IndexingPerformance,
    pub attestation_domain: &'static Eip712Domain,
    pub bad_indexers: &'static HashSet<Address>,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
}
