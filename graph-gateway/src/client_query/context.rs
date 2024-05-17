use std::collections::{HashMap, HashSet};

use alloy_primitives::Address;
use alloy_sol_types::Eip712Domain;
use eventuals::{Eventual, Ptr};
use gateway_common::types::Indexing;
use gateway_framework::{
    budgets::Budgeter,
    chains::Chains,
    network::{discovery::Status, indexing_performance::IndexingPerformance},
    reporting::KafkaClient,
    scalar::ReceiptSigner,
    topology::network::GraphNetwork,
};
use ordered_float::NotNan;
use tokio::sync::watch;
use url::Url;

use crate::indexer_client::IndexerClient;

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub receipt_signer: &'static ReceiptSigner,
    pub kafka_client: &'static KafkaClient,
    pub budgeter: &'static Budgeter,
    pub l2_gateway: Option<Url>,
    pub grt_per_usd: watch::Receiver<NotNan<f64>>,
    pub chains: &'static Chains,
    pub network: GraphNetwork,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    pub indexing_perf: IndexingPerformance,
    pub attestation_domain: &'static Eip712Domain,
    pub bad_indexers: &'static HashSet<Address>,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
}
