use crate::{
    block_constraints::{block_constraints, BlockConstraint},
    chains::{self, test::Provider, BlockCache},
    fisherman_client::*,
    indexer_client::*,
    kafka_client::{self, KafkaInterface},
    manifest_client::SubgraphInfo,
    price_automation::QueryBudgetFactors,
    query_engine::*,
    receipts::ReceiptPools,
};
use async_trait::async_trait;
use indexer_selection::{
    actor::{IndexerUpdate, Update},
    test_utils::{default_cost_model, test_allocation_id, TEST_KEY},
    BlockStatus, Context, IndexerError, IndexerInfo, Indexing, IndexingStatus, SecretKey,
    Selection, UnresolvedBlock,
};
use prelude::{
    buffer_queue::{self, QueueWriter},
    decimal, double_buffer,
    rand::thread_rng,
    test_utils::*,
    *,
};
use rand::{
    distributions, rngs::SmallRng, seq::SliceRandom as _, Rng as _, RngCore as _, SeedableRng as _,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    env, fmt,
    ops::RangeInclusive,
    sync::Arc,
};
use tokio::{self, sync::Mutex};

/// Query engine tests use pseudorandomly generated network state and client query as inputs.
/// The using these inputs, the query engine produces a query result that gets checked based on
/// properties of the inputs. The network state is represented by `Topology`, which models the
/// networks, subgraphs, deployments, and indexers in the Graph protocol. See also
/// `Topology::check_result`.

// TODO: gen topology deltas to test state changes

// This should be replaced by usize::log2 eventually
// https://github.com/rust-lang/rust/issues/70887
#[inline]
fn log_2(x: usize) -> usize {
    assert!(x > 0);
    (std::mem::size_of::<usize>() * 8) - x.leading_zeros() as usize - 1
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum TokenAmount {
    Zero,
    LessThanEnough,
    Enough,
    MoreThanEnough,
}

impl TokenAmount {
    fn as_udecimal<const P: u8>(&self, table: &[f64; 4]) -> decimal::UDecimal<P> {
        use TokenAmount::*;
        match self {
            Zero => table[0].to_string().parse().unwrap(),
            LessThanEnough => table[1].to_string().parse().unwrap(),
            Enough => table[2].to_string().parse().unwrap(),
            MoreThanEnough => table[3].to_string().parse().unwrap(),
        }
    }
}

struct Topology {
    config: TopologyConfig,
    rng: SmallRng,
    networks: BTreeMap<String, NetworkTopology>,
    indexers: BTreeMap<Address, IndexerTopology>,
    subgraphs: BTreeMap<SubgraphID, SubgraphTopology>,
}

#[derive(Clone)]
struct TopologyConfig {
    indexers: RangeInclusive<usize>,
    networks: RangeInclusive<usize>,
    blocks: RangeInclusive<usize>,
    subgraphs: RangeInclusive<usize>,
    deployments: RangeInclusive<usize>,
    indexings: RangeInclusive<usize>,
}

#[derive(Clone, Debug)]
struct SubgraphTopology {
    id: SubgraphID,
    deployments: Vec<DeploymentTopology>,
}

#[derive(Clone, Debug)]
struct NetworkTopology {
    name: String,
    blocks: Vec<BlockPointer>,
}

#[derive(Clone, Debug)]
struct DeploymentTopology {
    id: SubgraphDeploymentID,
    network: String,
    indexings: Vec<Address>,
}

#[derive(Clone, Debug)]
struct IndexerTopology {
    id: Address,
    staked_grt: TokenAmount,
    allocated_grt: TokenAmount,
    blocks_behind: usize,
    min_block: Option<u64>,
    fee: TokenAmount,
    indexer_err: bool,
    challenge_outcome: ChallengeOutcome,
}

impl IndexerTopology {
    fn block(&self, blocks: usize) -> u64 {
        if blocks == 0 {
            return 0;
        }
        (blocks - self.blocks_behind(blocks) - 1) as u64
    }

    fn blocks_behind(&self, blocks: usize) -> usize {
        if blocks == 0 {
            return 0;
        }
        self.blocks_behind % blocks
    }
}

impl Topology {
    fn new(config: TopologyConfig, rng: &SmallRng) -> Self {
        let mut topology = Self {
            config: config.clone(),
            rng: SmallRng::from_rng(rng.clone()).unwrap(),
            subgraphs: BTreeMap::new(),
            networks: BTreeMap::new(),
            indexers: BTreeMap::new(),
        };
        for _ in 0..topology.rng.gen_range(config.networks) {
            let network = topology.gen_network();
            topology.networks.insert(network.name.clone(), network);
        }
        for _ in 0..topology.rng.gen_range(config.indexers) {
            let indexer = topology.gen_indexer();
            topology.indexers.insert(indexer.id, indexer);
        }
        for _ in 0..topology.rng.gen_range(config.subgraphs) {
            let subgraph = topology.gen_subgraph();
            topology.subgraphs.insert(subgraph.id.clone(), subgraph);
        }
        topology
    }

    fn block_caches(&self) -> Arc<HashMap<String, BlockCache>> {
        let resolvers = self
            .networks
            .iter()
            .map(|(name, network)| {
                let provider = Provider {
                    network: name.clone(),
                    blocks: network.blocks.clone(),
                };
                let cache = BlockCache::new::<chains::test::Client>(provider);
                (name.clone(), cache)
            })
            .collect();
        Arc::new(resolvers)
    }

    fn gen_query(&mut self) -> Query {
        let (subgraph, deployment) = self
            .subgraphs
            .iter()
            .flat_map(|(s, t)| t.deployments.iter().map(|d| (s.clone(), d.clone())))
            .collect::<Vec<(SubgraphID, DeploymentTopology)>>()
            .choose(&mut self.rng)
            .unwrap()
            .clone();
        let network = self.networks.get(&deployment.network).unwrap();
        let query_body = if self.rng.gen_bool(0.01) {
            "?".to_string()
        } else {
            let constraints = match (self.rng.gen_bool(0.1), network.blocks.choose(&mut self.rng)) {
                (true, Some(block)) => format!("(block:{{number:{}}})", block.number),
                _ => "".to_string(),
            };
            format!("{{ entities{} {{ id }} }}", constraints)
        };
        let mut query = Query::new("".into(), query_body.into(), None);
        query.api_key = Some(Arc::new(APIKey::default()));
        query.subgraph = Some(Ptr::new(SubgraphInfo {
            id: subgraph,
            deployment: deployment.id,
            network: deployment.network,
            min_block: 0,
            features: vec![],
        }));
        query
    }

    fn gen_network(&mut self) -> NetworkTopology {
        let mut network = NetworkTopology {
            name: self.gen_str(log_2(*self.config.networks.end()).max(1)),
            blocks: Vec::new(),
        };
        let block_count = self.gen_len(self.config.blocks.clone());
        for i in 0..block_count {
            network.blocks.push(self.gen_block(i as u64));
        }
        network
    }

    fn gen_subgraph(&mut self) -> SubgraphTopology {
        let mut subgraph = SubgraphTopology {
            id: self.gen_bytes().into(),
            deployments: Vec::new(),
        };
        for _ in 0..self.gen_len(self.config.deployments.clone()) {
            subgraph.deployments.push(self.gen_deployment());
        }
        subgraph
    }

    fn gen_deployment(&mut self) -> DeploymentTopology {
        let network = self
            .networks
            .keys()
            .collect::<Vec<&String>>()
            .choose(&mut self.rng)
            .unwrap()
            .to_string();
        let mut deployment = DeploymentTopology {
            id: self.gen_bytes().into(),
            network,
            indexings: Vec::new(),
        };
        let indexers = self.indexers.keys().cloned().collect::<Vec<Address>>();
        for _ in 0..self.gen_len(self.config.indexings.clone()) {
            match indexers.choose(&mut self.rng) {
                None => break,
                Some(id) if deployment.indexings.contains(id) => continue,
                Some(id) => deployment.indexings.push(*id),
            }
        }
        deployment
    }

    fn gen_indexer(&mut self) -> IndexerTopology {
        let block_range = 0..=*self.config.blocks.end();
        let min_block = if self.rng.gen_bool(0.1) {
            Some(self.rng.gen_range(block_range.clone()) as u64)
        } else {
            None
        };
        IndexerTopology {
            id: self.gen_bytes().into(),
            staked_grt: self.gen_amount(),
            allocated_grt: self.gen_amount(),
            blocks_behind: self.rng.gen_range(block_range),
            min_block,
            fee: self.gen_amount(),
            indexer_err: self.rng.gen_bool(0.01),
            challenge_outcome: self.gen_challenge_outcome(),
        }
    }

    fn gen_block(&mut self, number: u64) -> BlockPointer {
        let hash = self.gen_bytes().into();
        BlockPointer { number, hash }
    }

    fn gen_amount(&mut self) -> TokenAmount {
        match self.rng.gen_range(0..=3) {
            0 => TokenAmount::Zero,
            1 => TokenAmount::LessThanEnough,
            2 => TokenAmount::Enough,
            3 => TokenAmount::MoreThanEnough,
            _ => unreachable!(),
        }
    }

    fn gen_challenge_outcome(&mut self) -> ChallengeOutcome {
        if self.rng.gen_bool(0.1) {
            *[
                ChallengeOutcome::FailedToProvideAttestation,
                ChallengeOutcome::DisagreeWithTrustedIndexer,
                ChallengeOutcome::DisagreeWithUntrustedIndexer,
            ]
            .choose(&mut self.rng)
            .unwrap()
        } else {
            *[
                ChallengeOutcome::AgreeWithTrustedIndexer,
                ChallengeOutcome::Unknown,
            ]
            .choose(&mut self.rng)
            .unwrap()
        }
    }

    fn gen_str(&mut self, len: usize) -> String {
        (&mut self.rng)
            .sample_iter(&distributions::Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    fn gen_bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut bytes = [0u8; N];
        self.rng.fill_bytes(&mut bytes);
        bytes
    }

    /// Generate a length in the given range that should only be zero a small fraction of the time.
    /// This is done to ensure that generated test cases have a reasonable probability to have
    /// input components necessary to execute a complete query, while also covering the zero cases.
    fn gen_len(&mut self, range: RangeInclusive<usize>) -> usize {
        if range.end() == &0 {
            return 0;
        }
        if range.contains(&0) && self.rng.gen_bool(0.05) {
            return 0;
        } else {
            loop {
                let n = self.rng.gen_range(range.clone());
                if n != 0 {
                    return n;
                }
            }
        }
    }

    fn deployments(&self) -> Vec<&DeploymentTopology> {
        self.subgraphs
            .iter()
            .flat_map(|(_, subgraph)| &subgraph.deployments)
            .collect()
    }

    fn indexings(&self, indexer: &Address) -> Vec<(&DeploymentTopology, &NetworkTopology)> {
        self.deployments()
            .into_iter()
            .filter(|deployment| deployment.indexings.contains(indexer))
            .map(|deployment| {
                let network = self.networks.get(&deployment.network).unwrap();
                (deployment, network)
            })
            .collect()
    }

    async fn write_inputs(
        &mut self,
        writer: &QueueWriter<Update>,
        receipt_pools: ReceiptPools,
    ) -> Eventual<Ptr<HashMap<SubgraphDeploymentID, Vec<Address>>>> {
        writer
            .write(Update::USDToGRTConversion("1.0".parse().unwrap()))
            .unwrap();
        writer
            .write(Update::SlashingPercentage("0.1".parse().unwrap()))
            .unwrap();

        let stake_table = [0.0, 50e3, 100e3, 150e3];
        let fee_table = [0.0, 0.1, 1.0, 2.0];
        let url = "http://localhost".parse::<URL>().unwrap();
        let indexers = self
            .indexers
            .values()
            .map(|indexer| {
                let info = Arc::new(IndexerInfo {
                    url: url.clone(),
                    stake: indexer.staked_grt.as_udecimal(&stake_table),
                });
                let cost_model = Some(Ptr::new(default_cost_model(
                    indexer.fee.as_udecimal(&fee_table),
                )));
                let indexings = self
                    .indexings(&indexer.id)
                    .into_iter()
                    .filter_map(|(deployment, network)| {
                        let update = IndexingStatus {
                            allocations: Arc::new(HashMap::from_iter([(
                                test_allocation_id(&indexer.id, &deployment.id),
                                indexer.allocated_grt.as_udecimal(&stake_table),
                            )])),
                            cost_model: cost_model.clone(),
                            block: Some(BlockStatus {
                                reported_number: indexer.block(network.blocks.len()),
                                blocks_behind: indexer.blocks_behind(network.blocks.len()) as u64,
                                behind_reported_block: false,
                                min_block: indexer.min_block,
                            }),
                        };
                        Some((deployment.id, update))
                    })
                    .collect::<HashMap<SubgraphDeploymentID, IndexingStatus>>();
                (indexer.id, IndexerUpdate { info, indexings })
            })
            .collect::<HashMap<Address, IndexerUpdate>>();

        let signer = SecretKey::from_str(TEST_KEY).unwrap();
        for (indexer, update) in &indexers {
            for (deployment, status) in &update.indexings {
                let indexing = Indexing {
                    indexer: *indexer,
                    deployment: *deployment,
                };
                receipt_pools
                    .update_receipt_pool(&signer, &indexing, &status.allocations)
                    .await;
            }
        }

        writer.write(Update::Indexers(indexers)).unwrap();

        let deployment_indexers = Eventual::from_value(Ptr::new(
            self.deployments()
                .iter()
                .map(|deployment| {
                    let indexers = deployment.indexings.iter().cloned().collect();
                    (deployment.id, indexers)
                })
                .collect(),
        ));
        eventuals::idle().await;
        deployment_indexers
    }

    fn check_result(
        &self,
        query: &Query,
        result: Result<(), QueryEngineError>,
    ) -> Result<(), Vec<String>> {
        use QueryEngineError::*;

        let mut trace = Vec::new();
        trace.push(format!("result: {:?}", result));
        trace.push(format!("{:#?}", query));

        let subgraph = query.subgraph.as_ref().unwrap();
        let deployment = self
            .deployments()
            .into_iter()
            .find(|deployment| &deployment.id == &subgraph.deployment)
            .unwrap();
        let indexers = deployment
            .indexings
            .iter()
            .map(|id| self.indexers.get(id).unwrap())
            .collect::<Vec<&IndexerTopology>>();

        if indexers.is_empty() {
            return Self::expect_err(&mut trace, &result, NoIndexers);
        }

        let context = match Context::new(&query.query, "") {
            Ok(context) => context,
            Err(_) => return Self::expect_err(&mut trace, &result, MalformedQuery),
        };
        let required_block = match block_constraints(&context).unwrap().into_iter().next() {
            None | Some(BlockConstraint::Unconstrained) => None,
            Some(BlockConstraint::Number(n)) => Some(n),
            Some(constraint) => unreachable!("unexpected constraint: {:?}", constraint),
        };

        let blocks = &self.networks.get(&subgraph.network).unwrap().blocks;
        if blocks.is_empty() {
            return Self::expect_err(
                &mut trace,
                &result,
                MissingBlock(UnresolvedBlock::WithNumber(0)),
            );
        }

        // Valid indexers have the following properties:
        let valid_indexer = |indexer: &IndexerTopology| -> bool {
            // no failure to indexing the subgraph
            !indexer.indexer_err
            // more than zero stake
            && (indexer.staked_grt > TokenAmount::Zero)
            // more than zero allocation
            && (indexer.allocated_grt > TokenAmount::Zero)
            // fee <= budget
            && (indexer.fee <= TokenAmount::Enough)
            // valid minimum block
            && required_block.and_then(|required| Some(indexer.min_block? <= required)).unwrap_or(true)
            // indexed required block
            && required_block.map(|required| indexer.block(blocks.len()) >= required).unwrap_or(true)
        };
        let valid = indexers
            .iter()
            .cloned()
            .filter(|indexer| valid_indexer(indexer))
            .collect::<Vec<&IndexerTopology>>();
        trace.push(format!("valid indexers: {:#?}", valid));

        if query.indexer_attempts.is_empty() {
            if !valid.is_empty() {
                return Self::err_with(
                    &mut trace,
                    format!("expected no valid indexer, got {}", valid.len()),
                );
            }
            if matches!(result, Err(NoIndexerSelected) | Err(FeesTooHigh(_))) {
                return Ok(());
            }
            return Self::err_with(
                &mut trace,
                format!("expected no valid indexers, got {:?}", result),
            );
        }

        let mut failed_attempts = query.indexer_attempts.clone();
        let last_attempt = failed_attempts.pop();
        let success_check = last_attempt
            .as_ref()
            .and_then(|attempt| match &attempt.result {
                Ok(_) => Some(self.check_successful_attempt(&mut trace, &result, &valid, &attempt)),
                Err(_) => None,
            });
        match last_attempt {
            Some(attempt) if success_check.is_none() => failed_attempts.push(attempt),
            _ => (),
        };
        for attempt in &failed_attempts {
            if let Err(trace) = self.check_failed_attempt(&mut trace, &indexers, &valid, &attempt) {
                return Err(trace);
            }
        }
        match success_check {
            Some(result) => result,
            None => Ok(()),
        }
    }

    fn check_failed_attempt(
        &self,
        trace: &mut Vec<String>,
        indexers: &[&IndexerTopology],
        valid: &[&IndexerTopology],
        attempt: &IndexerAttempt,
    ) -> Result<(), Vec<String>> {
        if attempt.result.is_ok() {
            return Self::err_with(
                trace,
                format!("expected indexer query error, got: {:#?}", attempt),
            );
        }
        if !indexers.iter().any(|indexer| indexer.id == attempt.indexer) {
            return Self::err_with(
                trace,
                format!("attempted indexer not available: {:?}", attempt.indexer),
            );
        }
        if valid.iter().any(|indexer| indexer.id == attempt.indexer) {
            return Self::err_with(
                trace,
                format!("expected invalid indexer attempt, got {:#?}", attempt),
            );
        }
        Ok(())
    }

    fn check_successful_attempt(
        &self,
        trace: &mut Vec<String>,
        result: &Result<(), QueryEngineError>,
        valid: &[&IndexerTopology],
        attempt: &IndexerAttempt,
    ) -> Result<(), Vec<String>> {
        if let Err(err) = result {
            return Self::err_with(trace, format!("expected success, got {:?}", err));
        }
        let response = match &attempt.result {
            Ok(response) => response,
            Err(err) => return Self::err_with(trace, format!("expected response, got {:?}", err)),
        };
        if !response.payload.contains("success") {
            return Self::err_with(trace, format!("expected success, got {}", response.payload));
        }
        if !valid.iter().any(|indexer| attempt.indexer == indexer.id) {
            return Self::err_with(trace, "response did not match any valid indexer");
        }
        Ok(())
    }

    fn expect_err(
        trace: &mut Vec<String>,
        result: &Result<(), QueryEngineError>,
        err: QueryEngineError,
    ) -> Result<(), Vec<String>> {
        match result {
            Err(e) if e == &err => Ok(()),
            _ => Self::err_with(trace, format!("expected {:?}, got {:?}", err, result)),
        }
    }

    fn err_with<S: ToString>(trace: &mut Vec<String>, err: S) -> Result<(), Vec<String>> {
        trace.push(err.to_string());
        Err(trace.clone())
    }
}

#[derive(Clone)]
struct TopologyIndexer {
    topology: Arc<Mutex<Topology>>,
}

#[async_trait]
impl IndexerInterface for TopologyIndexer {
    async fn query_indexer(
        &self,
        selection: &Selection,
        query: String,
        _receipt: &[u8],
    ) -> Result<IndexerResponse, IndexerError> {
        use regex::Regex;
        let topology = self.topology.lock().await;
        let indexer = topology.indexers.get(&selection.indexing.indexer).unwrap();
        if indexer.indexer_err {
            return Err(IndexerError::Other("indexer error".to_string()));
        }
        let deployment = topology
            .deployments()
            .into_iter()
            .find(|d| &d.id == &selection.indexing.deployment)
            .unwrap();
        let blocks = &topology.networks.get(&deployment.network).unwrap().blocks;

        let matcher = Regex::new(r#"block: \{hash: \\"0x([[:xdigit:]]+)\\"}"#).unwrap();
        for capture in matcher.captures_iter(&query) {
            let hash = capture.get(1).unwrap().as_str().parse::<Bytes32>().unwrap();
            let number = blocks.iter().position(|block| block.hash == hash).unwrap() as u64;
            if number > indexer.block(blocks.len()) {
                return Err(IndexerError::Other(json!({
                    "errors": vec![json!({
                        "message": "Failed to decode `block.hash` value: `no block with that hash found`",
                    })]
                }).to_string()));
            }
        }
        Ok(IndexerResponse {
            status: 200,
            payload: r#"{"data": "success"}"#.into(),
            attestation: Some(Attestation {
                request_cid: Bytes32::default(),
                response_cid: Bytes32::default(),
                deployment: Bytes32::from(*selection.indexing.deployment),
                v: 0,
                r: Bytes32::default(),
                s: Bytes32::default(),
            }),
        })
    }
}

struct DummyKafka;

impl KafkaInterface for DummyKafka {
    fn send<M: kafka_client::Msg>(&self, _: &M) {}
}

#[derive(Clone)]
struct TopologyFisherman {
    topology: Arc<Mutex<Topology>>,
}

#[async_trait]
impl FishermanInterface for TopologyFisherman {
    async fn challenge(
        &self,
        indexer: &Address,
        _: &Address,
        _: &str,
        _: &Attestation,
    ) -> ChallengeOutcome {
        self.topology
            .lock()
            .await
            .indexers
            .get(indexer)
            .unwrap()
            .challenge_outcome
    }
}

impl fmt::Debug for Topology {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "subgraphs: {:#?}\nindexers: {:#?}\nnetworks: {:#?}",
            self.subgraphs, self.indexers, self.networks
        )
    }
}

#[tokio::test]
async fn query_engine() {
    init_test_tracing();
    let seed = env::vars()
        .find(|(k, _)| k == "TEST_SEED")
        .and_then(|(_, v)| v.parse::<u64>().ok())
        .unwrap_or(thread_rng().next_u64());
    tracing::info!(%seed);
    let rng = SmallRng::seed_from_u64(seed);
    for _ in 0..10 {
        let (update_writer, update_reader) = buffer_queue::pair();
        let (isa_state, isa_writer) = double_buffer!(indexer_selection::State::default());
        let topology = Arc::new(Mutex::new(Topology::new(
            TopologyConfig {
                indexers: 5..=10,
                networks: 1..=3,
                blocks: 0..=5,
                subgraphs: 1..=3,
                deployments: 1..=3,
                indexings: 0..=3,
            },
            &rng,
        )));
        let block_caches = topology.lock().await.block_caches();
        let receipt_pools = ReceiptPools::default();
        let deployment_indexers = topology
            .lock()
            .await
            .write_inputs(&update_writer, receipt_pools.clone())
            .await;
        let query_engine = QueryEngine {
            config: Config {
                indexer_selection_retry_limit: 3,
                budget_factors: QueryBudgetFactors {
                    scale: 1.0,
                    discount: 0.0,
                    processes: 1.0,
                },
            },
            indexer_client: TopologyIndexer {
                topology: topology.clone(),
            },
            kafka_client: Arc::new(DummyKafka),
            fisherman_client: Some(Arc::new(TopologyFisherman {
                topology: topology.clone(),
            })),
            deployment_indexers,
            block_caches,
            receipt_pools: receipt_pools.clone(),
            isa: isa_state.clone(),
            observations: update_writer.clone(),
        };
        tokio::spawn(async move {
            indexer_selection::actor::process_updates(isa_writer, update_reader).await;
            unreachable!();
        });
        for _ in 0..10 {
            update_writer.flush().await.unwrap();
            let mut query = topology.lock().await.gen_query();
            let result = query_engine.execute_query(&mut query).await;
            let trace = match topology.lock().await.check_result(&query, result) {
                Err(trace) => trace,
                Ok(()) => continue,
            };
            println!("{:#?}", topology.lock().await);
            for line in trace {
                println!("{}", line);
            }
            panic!("test failed");
        }
    }
}
