use std::collections::{HashMap, HashSet};

use alloy_primitives::Address;
use alloy_sol_types::Eip712Domain;
use eventuals::{Eventual, Ptr};
use ordered_float::NotNan;
use toolshed::url::Url;

use gateway_common::types::Indexing;
use gateway_framework::budgets::Budgeter;
use gateway_framework::chains::BlockCache;

use crate::indexer_client::IndexerClient;
use crate::indexers::indexing;
use crate::indexing_performance::IndexingPerformance;
use crate::reports::KafkaClient;
use crate::topology::GraphNetwork;

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub kafka_client: &'static KafkaClient,
    pub budgeter: &'static Budgeter,
    pub indexer_selection_retry_limit: usize,
    pub l2_gateway: Option<Url>,
    pub grt_per_usd: Eventual<NotNan<f64>>,
    pub block_caches: &'static HashMap<String, &'static BlockCache>,
    pub network: GraphNetwork,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, indexing::Status>>>,
    pub indexing_perf: IndexingPerformance,
    pub attestation_domain: &'static Eip712Domain,
    pub bad_indexers: &'static HashSet<Address>,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
}
