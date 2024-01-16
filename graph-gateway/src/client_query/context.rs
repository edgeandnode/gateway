use std::collections::{HashMap, HashSet};

use alloy_primitives::Address;
use alloy_sol_types::Eip712Domain;
use eventuals::{Eventual, Ptr};
use toolshed::buffer_queue::QueueWriter;
use toolshed::double_buffer::DoubleBufferReader;
use toolshed::url::Url;

use gateway_common::types::Indexing;
use gateway_framework::budgets::Budgeter;
use gateway_framework::chains::BlockCache;
use indexer_selection::actor::Update;

use crate::auth::AuthHandler;
use crate::indexer_client::IndexerClient;
use crate::indexers::indexing;
use crate::reports::KafkaClient;
use crate::topology::GraphNetwork;

#[derive(Clone)]
pub struct Context {
    pub indexer_client: IndexerClient,
    pub kafka_client: &'static KafkaClient,
    pub graph_env_id: String,
    pub auth_handler: &'static AuthHandler,
    pub budgeter: &'static Budgeter,
    pub indexer_selection_retry_limit: usize,
    pub l2_gateway: Option<Url>,
    pub block_caches: &'static HashMap<String, &'static BlockCache>,
    pub network: GraphNetwork,
    pub indexing_statuses: Eventual<Ptr<HashMap<Indexing, indexing::Status>>>,
    pub attestation_domain: &'static Eip712Domain,
    pub bad_indexers: &'static HashSet<Address>,
    pub indexings_blocklist: Eventual<Ptr<HashSet<Indexing>>>,
    pub isa_state: DoubleBufferReader<indexer_selection::State>,
    pub observations: QueueWriter<Update>,
}
