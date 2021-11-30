use crate::{
    indexer_client::*,
    indexer_selection::{
        test_utils::{default_cost_model, TEST_KEY},
        IndexingStatus, SecretKey,
    },
    manifest_client::{SubgraphInfo, SubgraphInfoMap},
    prelude::{decimal, test_utils::*, *},
    query_engine::*,
};
use async_trait::async_trait;
use rand::{
    distributions,
    rngs::{OsRng, SmallRng},
    seq::SliceRandom,
    Rng, RngCore as _, SeedableRng,
};
use std::{collections::BTreeMap, env, error::Error, fmt, iter, ops::RangeInclusive};
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
    inputs: InputWriters,
    rng: SmallRng,
    indexers: BTreeMap<Address, IndexerTopology>,
    networks: BTreeMap<String, NetworkTopology>,
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
struct NetworkTopology {
    name: String,
    blocks: Vec<BlockPointer>,
    subgraphs: BTreeMap<String, SubgraphTopology>,
}

#[derive(Clone, Debug)]
struct SubgraphTopology {
    name: String,
    deployments: Vec<DeploymentTopology>,
}

#[derive(Clone, Debug)]
struct DeploymentTopology {
    id: SubgraphDeploymentID,
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
    fn new(config: TopologyConfig, inputs: InputWriters, rng: &SmallRng) -> Self {
        let mut topology = Self {
            config: config.clone(),
            inputs,
            rng: SmallRng::from_rng(rng.clone()).unwrap(),
            indexers: BTreeMap::new(),
            networks: BTreeMap::new(),
        };
        for _ in 0..topology.rng.gen_range(config.indexers) {
            let indexer = topology.gen_indexer();
            topology.indexers.insert(indexer.id, indexer);
        }
        for _ in 0..topology.rng.gen_range(config.networks) {
            let network = topology.gen_network();
            topology.networks.insert(network.name.clone(), network);
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

    fn subgraph_info(&self) -> SubgraphInfoMap {
        let info = self
            .subgraphs()
            .into_iter()
            .flat_map(|(network, subgraph)| {
                subgraph
                    .deployments
                    .iter()
                    .map(|deployment| {
                        let info = Eventual::from_value(Ptr::new(SubgraphInfo {
                            id: deployment.id.clone(),
                            network: network.name.clone(),
                            features: vec![],
                        }));
                        (deployment.id.clone(), info)
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        Eventual::from_value(Ptr::new(info))
    }

    fn gen_query(&mut self) -> ClientQuery {
        fn choose_name<'s, I: IntoIterator<Item = &'s String>>(
            rng: &mut SmallRng,
            names: I,
            invalid: &str,
        ) -> String {
            names
                .into_iter()
                .collect::<Vec<&String>>()
                .choose(rng)
                .map(|&name| name.clone())
                .unwrap_or(invalid.into())
        }

        let invalid_network = self.gen_str(*self.config.networks.end() + 1);
        let invalid_subgraph = self.gen_str(*self.config.subgraphs.end() + 1);
        let rng = &mut self.rng;
        let mut network = choose_name(rng, self.networks.keys(), &invalid_network);
        let mut subgraph = self
            .networks
            .get(&network)
            .map(|net| choose_name(rng, net.subgraphs.keys(), &invalid_subgraph))
            .unwrap_or(invalid_subgraph.clone());
        if self.flip_coin(32) {
            network = invalid_network;
        }
        if self.flip_coin(32) {
            subgraph = invalid_subgraph;
        }
        let query = if self.flip_coin(32) { "?" } else { BASIC_QUERY };
        ClientQuery {
            id: QueryID::new(),
            api_key: Arc::new(APIKey::default()),
            query: query.into(),
            variables: None,
            network,
            subgraph: Subgraph::Name(subgraph),
        }
    }

    fn gen_network(&mut self) -> NetworkTopology {
        let mut network = NetworkTopology {
            name: self.gen_str(log_2(*self.config.networks.end())),
            blocks: Vec::new(),
            subgraphs: BTreeMap::new(),
        };
        let block_count = self.gen_len(self.config.blocks.clone(), 32);
        for i in 0..block_count {
            network.blocks.push(self.gen_block(i as u64));
        }
        for _ in 0..self.gen_len(self.config.subgraphs.clone(), 32) {
            let subgraph = self.gen_subgraph();
            network.subgraphs.insert(subgraph.name.clone(), subgraph);
        }
        network
    }

    fn gen_subgraph(&mut self) -> SubgraphTopology {
        let mut name = self.gen_str(log_2(*self.config.networks.end()));
        // TODO: For now, subgraph names must be unique across networks
        while self
            .subgraphs()
            .iter()
            .any(|(_, subgraph)| subgraph.name == name)
        {
            name = self.gen_str(log_2(*self.config.networks.end()));
        }

        let mut subgraph = SubgraphTopology {
            name,
            deployments: Vec::new(),
        };
        for _ in 1..self.gen_len(self.config.deployments.clone(), 32) {
            subgraph.deployments.push(DeploymentTopology {
                id: self.gen_bytes().into(),
                indexings: vec![],
            });
        }
        subgraph.deployments.push(self.gen_deployment());
        subgraph
    }

    fn gen_deployment(&mut self) -> DeploymentTopology {
        let mut deployment = DeploymentTopology {
            id: self.gen_bytes().into(),
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

    fn subgraph(&self, network: &str, subgraph: &str) -> Option<&SubgraphTopology> {
        self.networks
            .get(network)
            .and_then(|net| net.subgraphs.get(subgraph))
    }

    fn subgraphs(&self) -> Vec<(NetworkTopology, SubgraphTopology)> {
        self.networks
            .values()
            .flat_map(|net| iter::repeat(net.clone()).zip(net.subgraphs.values().cloned()))
            .collect()
    }

    fn indexings(
        &self,
    ) -> Vec<(
        NetworkTopology,
        SubgraphTopology,
        DeploymentTopology,
        IndexerTopology,
    )> {
        self.networks
            .values()
            .flat_map(|net| iter::repeat(net.clone()).zip(net.subgraphs.values()))
            .flat_map(|(net, subgraph)| {
                let path = iter::repeat((net, subgraph.clone()));
                path.zip(subgraph.deployments.iter())
            })
            .flat_map(|((net, subgraph), deployment)| {
                let path = iter::repeat((net, subgraph, deployment.clone()));
                path.zip(deployment.indexings.iter())
            })
            .map(|((net, subgraph, deployment), indexing)| {
                let indexer = self.indexers.get(indexing).unwrap().clone();
                (net, subgraph, deployment, indexer)
            })
            .collect()
    }

    async fn write_inputs(&mut self) {
        let indexings = self.indexings();
        let indexer_inputs = &mut self.inputs.indexer_inputs;
        indexer_inputs
            .slashing_percentage
            .write("0.1".parse().unwrap());
        indexer_inputs
            .usd_to_grt_conversion
            .write("1.0".parse().unwrap());
        let stake_table = [0.0, 50e3, 100e3, 150e3];
        for indexer in self.indexers.values() {
            let indexer_writer = indexer_inputs.indexers.write(&indexer.id).await;
            indexer_writer.url.write("".into());
            indexer_writer
                .stake
                .write(indexer.staked_grt.as_udecimal(&stake_table));
        }
        let test_key = SecretKey::from_str(TEST_KEY).unwrap();
        for (network, _, deployment, indexer) in indexings.iter() {
            let indexing = Indexing {
                deployment: deployment.id,
                indexer: indexer.id,
            };
            let fee = indexer.fee.as_udecimal(&[0.0, 0.1, 1.0, 2.0]);
            let indexing_writer = indexer_inputs.indexings.write(&indexing).await;
            indexing_writer
                .add_allocation(
                    Address::default(),
                    test_key.clone(),
                    indexer.allocated_grt.as_udecimal(&stake_table),
                )
                .await;
            indexing_writer.cost_model.write(default_cost_model(fee));
            if let Some(latest) = network.blocks.last() {
                indexing_writer.status.write(IndexingStatus {
                    block: indexer.block(network.blocks.len()) as u64,
                    latest: latest.number,
                });
            }
        }
        self.inputs.current_deployments.write(Ptr::new(
            self.subgraphs()
                .into_iter()
                .filter_map(|(_, subgraph)| Some((subgraph.name, subgraph.deployments.last()?.id)))
                .collect(),
        ));
        self.inputs.deployment_indexers.write(Ptr::new(
            self.subgraphs()
                .into_iter()
                .flat_map(|(_, subgraph)| subgraph.deployments)
                .map(|deployment| {
                    let indexings = deployment.indexings.iter().cloned().collect();
                    (deployment.id, indexings)
                })
                .collect(),
        ));
        eventuals::idle().await;
    }

    async fn check_result(
        &self,
        query: ClientQuery,
        result: Result<QueryResponse, QueryEngineError>,
    ) -> Result<(), Vec<String>> {
        fn err_with<S: ToString>(mut trace: Vec<String>, err: S) -> Result<(), Vec<String>> {
            trace.push(err.to_string());
            Err(trace.clone())
        }
        let mut trace = Vec::new();
        trace.push(format!("{:#?}", query));
        trace.push(format!("{:#?}", result));
        let subgraph_name = match &query.subgraph {
            Subgraph::Name(name) => name,
            Subgraph::Deployment(_) => panic!("Unexpected SubgraphDeploymentID"),
        };
        // Return SubgraphNotFound if the subgraph does not exist.
        let subgraph = match self.subgraph(&query.network, subgraph_name) {
            Some(subgraph) => subgraph,
            None => {
                if let Err(QueryEngineError::SubgraphNotFound) = result {
                    return Ok(());
                }
                // TODO: We currently assume that the network is mainnet, so these failures are not
                // expected. In the future there must be a check that the query network matches
                // with the expected network of the subgraph.
                if self.subgraphs().iter().all(|(network, subgraph)| {
                    (network.name != query.network) || (&subgraph.name != subgraph_name)
                }) {
                    return Ok(());
                }

                trace.push(format!("expected SubgraphNotFound, got {:#?}", result));
                return Err(trace);
            }
        };
        // Return MissingBlock if the network has no blocks.
        if let Err(QueryEngineError::MissingBlock(_)) = result {
            if self.networks.get(&query.network).unwrap().blocks.is_empty() {
                return Ok(());
            }
            return err_with(trace, format!("expected MissingBlock, got {:?}", result));
        }
        trace.push(format!("{:#?}", subgraph));
        let indexers = subgraph
            .deployments
            .last()
            .iter()
            .flat_map(|deployment| deployment.indexings.iter())
            .map(|id| self.indexers.get(id).unwrap())
            .collect::<Vec<&IndexerTopology>>();
        // Valid indexers have more than zero stake, more than zero allocation, and fee <= budget.
        fn valid_indexer(indexer: &IndexerTopology) -> bool {
            !indexer.indexer_err
                && (indexer.staked_grt > TokenAmount::Zero)
                && (indexer.allocated_grt > TokenAmount::Zero)
                && (indexer.fee <= TokenAmount::Enough)
        }
        // Return NoIndexers if no valid indexers exist.
        if let Err(QueryEngineError::NoIndexers) = result {
            return Ok(());
        }
        // Return MalformedQuery Malformed queries are rejected.
        if query.query == "?" {
            if let Err(QueryEngineError::MalformedQuery) = result {
                return Ok(());
            }
            return err_with(trace, format!("expected MalformedQuery, got {:#?}", result));
        }
        let valid = indexers
            .iter()
            .cloned()
            .filter(|indexer| valid_indexer(indexer))
            .collect::<Vec<&IndexerTopology>>();
        trace.push(format!("valid indexers: {:#?}", valid));
        if valid.len() > 0 {
            // A valid indexer implies that a response is returned.
            let response = match result {
                Ok(response) => response,
                Err(err) => return err_with(trace, format!("expected response, got {:?}", err)),
            };
            // The test resolver only gives the following response for successful queries.
            if !response.response.payload.contains("success") {
                return err_with(
                    trace,
                    format!("expected success, got {:#?}", response.response),
                );
            }
            // The response is from a valid indexer.
            if let Some(_) = valid
                .into_iter()
                .find(|indexer| response.query.indexing.indexer == indexer.id)
            {
                return Ok(());
            }
            return err_with(trace, "response did not match any valid indexer");
        }
        // Return NoIndexerSelected if no valid indexers were found.
        if let Err(QueryEngineError::NoIndexerSelected) = result {
            return Ok(());
        }
        err_with(
            trace,
            format!("expected NoIndexerSelected, got {:#?}", result),
        )
    }
}

#[derive(Clone)]
struct TopologyIndexer {
    topology: Arc<Mutex<Topology>>,
}

#[async_trait]
impl IndexerInterface for TopologyIndexer {
    async fn query_indexer(&self, query: &IndexerQuery) -> Result<IndexerResponse, Box<dyn Error>> {
        use regex::Regex;
        let topology = self.topology.lock().await;
        let indexer = topology.indexers.get(&query.indexing.indexer).unwrap();
        if indexer.indexer_err {
            return Err("indexer error".into());
        }
        let blocks = &topology.networks.get(&query.network).unwrap().blocks;
        let matcher = Regex::new(r#"block: \{hash: \\"0x([[:xdigit:]]+)\\"}"#).unwrap();
        for capture in matcher.captures_iter(&query.query) {
            let hash = capture.get(1).unwrap().as_str().parse::<Bytes32>().unwrap();
            let number = blocks.iter().position(|block| block.hash == hash).unwrap();
            if number > indexer.block(blocks.len()) {
                todo!("block ahead of indexer")
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

impl fmt::Debug for Topology {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "indexers: {:#?}\nnetworks: {:#?}",
            self.indexers, self.networks
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
        let (input_writers, inputs) = Inputs::new();
        let topology = Arc::new(Mutex::new(Topology::new(
            TopologyConfig {
                indexers: 5..=10,
                networks: 0..=3,
                blocks: 0..=5,
                subgraphs: 0..=3,
                deployments: 0..=3,
                indexings: 0..=3,
            },
            input_writers,
            &rng,
        )));
        let resolvers = topology.lock().await.resolvers();
        let subgraph_info = topology.lock().await.subgraph_info();
        let query_engine = QueryEngine::new(
            Config {
                network: "test".to_string(),
                indexer_selection_retry_limit: 3,
                utility: UtilityConfig::default(),
                query_budget: 1u64.try_into().unwrap(),
            },
            TopologyIndexer {
                topology: topology.clone(),
            },
            resolvers,
            subgraph_info,
            inputs,
        );
        topology.lock().await.write_inputs().await;
        for _ in 0..100 {
            let query = topology.lock().await.gen_query();
            let result = query_engine.execute_query(query.clone()).await;
            let trace = match topology.lock().await.check_result(query, result).await {
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
