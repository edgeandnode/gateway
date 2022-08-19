use crate::{
    fisherman_client::*,
    indexer_client::*,
    indexer_selection::{
        actor::{test_process_updates, IndexerUpdate},
        test_utils::{default_cost_model, TEST_KEY},
        IndexerError, IndexerInfo, IndexingStatus,
    },
    kafka_client::{self, KafkaInterface},
    manifest_client::SubgraphInfo,
    prelude::{decimal, test_utils::*, *},
    query_engine::*,
    utils::{buffer_queue, double_buffer::double_buffer},
};
use async_trait::async_trait;
use rand::{
    distributions,
    rngs::{OsRng, SmallRng},
    seq::SliceRandom,
    Rng, RngCore as _, SeedableRng,
};
use serde_json::json;
use std::{collections::BTreeMap, env, fmt, ops::RangeInclusive};
use tokio::{self, sync::Mutex};
use url::Url;

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
    fee: TokenAmount,
    indexer_err: bool,
    challenge_outcome: ChallengeOutcome,
}

impl IndexerTopology {
    fn block(&self, blocks: usize) -> usize {
        if blocks == 0 {
            return 0;
        }
        blocks - self.blocks_behind(blocks) - 1
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

    fn resolvers(&self) -> Arc<HashMap<String, BlockResolver>> {
        let resolvers = self
            .networks
            .iter()
            .map(|(name, network)| (name.clone(), BlockResolver::test(&network.blocks)))
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
        let query_body = if self.flip_coin(32) { "?" } else { BASIC_QUERY };
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
            name: self.gen_str(log_2(*self.config.networks.end())),
            blocks: Vec::new(),
        };
        let block_count = self.gen_len(self.config.blocks.clone(), 32);
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
        for _ in 0..self.gen_len(self.config.deployments.clone(), 32) {
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
        for _ in 0..self.gen_len(self.config.indexings.clone(), 32) {
            match indexers.choose(&mut self.rng) {
                None => break,
                Some(id) if deployment.indexings.contains(id) => continue,
                Some(id) => deployment.indexings.push(*id),
            }
        }
        deployment
    }

    fn gen_indexer(&mut self) -> IndexerTopology {
        IndexerTopology {
            id: self.gen_bytes().into(),
            staked_grt: self.gen_amount(),
            allocated_grt: self.gen_amount(),
            blocks_behind: self.rng.gen_range(0..=*self.config.blocks.end()),
            fee: self.gen_amount(),
            indexer_err: self.flip_coin(16),
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
        if self.flip_coin(16) {
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

    #[inline]
    fn flip_coin(&mut self, fraction: u64) -> bool {
        (self.rng.next_u64() % fraction) == 0
    }

    /// Generate a length in the given range that should only be zero a small fraction of the time.
    /// This is done to ensure that generated test cases have a reasonable probability to have
    /// input components necessary to execute a complete query, while also covering the zero cases.
    fn gen_len(&mut self, range: RangeInclusive<usize>, zero_fraction: u64) -> usize {
        if range.end() == &0 {
            return 0;
        }
        if range.contains(&0) && self.flip_coin(zero_fraction) {
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

    fn deployments(&self) -> Vec<DeploymentTopology> {
        self.subgraphs
            .iter()
            .flat_map(|(_, subgraph)| subgraph.deployments.clone())
            .collect()
    }

    fn indexings(&self) -> Vec<(DeploymentTopology, IndexerTopology, NetworkTopology)> {
        self.deployments()
            .into_iter()
            .flat_map(|deployment| {
                let network = self.networks.get(&deployment.network).unwrap();
                deployment
                    .indexings
                    .clone()
                    .into_iter()
                    .map(move |indexer| {
                        let indexer = self.indexers.get(&indexer).unwrap().clone();
                        (deployment.clone(), indexer, network.clone())
                    })
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
        let url = "http://localhost".parse::<Url>().unwrap();
        let indexers = self
            .indexers
            .values()
            .map(|indexer| {
                let info = Arc::new(IndexerInfo {
                    url: url.clone(),
                    stake: indexer.staked_grt.as_udecimal(&stake_table),
                });
                let indexings = self
                    .indexings()
                    .into_iter()
                    .filter_map(|(deployment, indexer, network)| {
                        let update = IndexingStatus {
                            allocations: Arc::new(HashMap::from_iter([(
                                Address::default(),
                                indexer.allocated_grt.as_udecimal(&stake_table),
                            )])),
                            cost_model: Some(Ptr::new(default_cost_model(
                                indexer.fee.as_udecimal(&fee_table),
                            ))),
                            block: indexer.block(network.blocks.len()) as u64,
                            latest: network.blocks.last()?.number,
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

        // Valid indexers have the following properties:
        fn valid_indexer(indexer: &IndexerTopology) -> bool {
            // no failure to indexing the subgraph
            !indexer.indexer_err
            // more than zero stake
            && (indexer.staked_grt > TokenAmount::Zero)
            // more than zero allocation
            && (indexer.allocated_grt > TokenAmount::Zero)
            // fee <= budget
            && (indexer.fee <= TokenAmount::Enough)
        }
        let valid = indexers
            .iter()
            .cloned()
            .filter(|indexer| valid_indexer(indexer))
            .collect::<Vec<&IndexerTopology>>();
        trace.push(format!("valid indexers: {:#?}", valid));

        if query.indexer_attempts.is_empty() {
            return self
                .check_no_attempts(&mut trace, query, &result, &subgraph, &indexers, &valid);
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

    fn check_no_attempts(
        &self,
        trace: &mut Vec<String>,
        query: &Query,
        result: &Result<(), QueryEngineError>,
        subgraph: &SubgraphInfo,
        indexers: &[&IndexerTopology],
        valid: &[&IndexerTopology],
    ) -> Result<(), Vec<String>> {
        use QueryEngineError::*;
        if indexers.is_empty() {
            return Self::expect_err(trace, result, NoIndexers);
        }
        if query.query.as_ref() == "?" {
            return Self::expect_err(trace, result, MalformedQuery);
        }
        if self
            .networks
            .get(&subgraph.network)
            .unwrap()
            .blocks
            .is_empty()
        {
            return Self::expect_err(trace, result, MissingBlock(UnresolvedBlock::WithNumber(0)));
        }

        if !valid.is_empty() {
            return Self::err_with(
                trace,
                format!("expected no valid indexer, got {}", valid.len()),
            );
        }

        let high_fee_count = indexers
            .iter()
            .filter(|indexer| indexer.fee > TokenAmount::Enough)
            .count();
        if high_fee_count > 0 {
            return Self::expect_err(trace, result, QueryEngineError::FeesTooHigh(high_fee_count));
        }
        Self::expect_err(trace, result, NoIndexerSelected)
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
        query: &IndexerQuery,
        _receipt: &[u8],
    ) -> Result<IndexerResponse, IndexerError> {
        use regex::Regex;
        let topology = self.topology.lock().await;
        let indexer = topology.indexers.get(&query.indexing.indexer).unwrap();
        if indexer.indexer_err {
            return Err(IndexerError::Other("indexer error".to_string()));
        }
        let blocks = &topology.networks.get(&query.network).unwrap().blocks;
        let matcher = Regex::new(r#"block: \{hash: \\"0x([[:xdigit:]]+)\\"}"#).unwrap();
        for capture in matcher.captures_iter(&query.query) {
            let hash = capture.get(1).unwrap().as_str().parse::<Bytes32>().unwrap();
            let number = blocks.iter().position(|block| block.hash == hash).unwrap();
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
                deployment: Bytes32::from(*query.indexing.deployment),
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
async fn test() {
    init_test_tracing();
    let seed = env::vars()
        .find(|(k, _)| k == "TEST_SEED")
        .and_then(|(_, v)| v.parse::<u64>().ok())
        .unwrap_or(OsRng.next_u64());
    tracing::info!(%seed);
    let rng = SmallRng::seed_from_u64(seed);
    for _ in 0..100 {
        let (update_writer, mut update_reader) = buffer_queue::pair();
        let (isa_state, mut isa_writer) = double_buffer!(indexer_selection::State::default());
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
        let block_resolvers = topology.lock().await.resolvers();
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
            block_resolvers,
            receipt_pools: receipt_pools.clone(),
            isa: isa_state.clone(),
            observations: update_writer,
        };
        for _ in 0..100 {
            test_process_updates(&mut isa_writer, &mut update_reader).await;
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
