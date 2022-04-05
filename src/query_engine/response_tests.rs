use crate::{
  fisherman_client::FishermanClient,
  indexer_client::IndexerResponse,
  indexer_selection::{test_utils::*, IndexerError, IndexingStatus},
  kafka_client,
  prelude::{test_utils::*, *},
  query_engine::*,
};
use async_trait::async_trait;
use plotters::prelude::*;
use rand::{rngs::SmallRng, Rng as _, SeedableRng as _};
use secp256k1::SecretKey;
use std::collections::BTreeMap;

struct DummyKafka;

impl KafkaInterface for DummyKafka {
  fn send<M: kafka_client::Msg>(&self, _: &M) {}
}

#[derive(Clone)]
struct SimIndexers {
  indexer_timeout_rates: BTreeMap<Address, f64>,
  add_delay: Arc<Mutex<bool>>,
  rng: Arc<Mutex<SmallRng>>,
}

impl SimIndexers {
  fn new(indexer_timeout_rates: BTreeMap<Address, f64>, add_delay: Arc<Mutex<bool>>) -> Self {
    Self {
      indexer_timeout_rates,
      add_delay,
      rng: Arc::new(Mutex::new(SmallRng::from_entropy())),
    }
  }
}

#[async_trait]
impl IndexerInterface for SimIndexers {
  async fn query_indexer(&self, query: &IndexerQuery) -> Result<IndexerResponse, IndexerError> {
    if *self.add_delay.lock().await {
      let timeout_rate = *self
        .indexer_timeout_rates
        .get(&query.indexing.indexer)
        .unwrap();
      if self.rng.lock().await.gen_range(0.0..1.0) < timeout_rate {
        return Err(IndexerError::Timeout);
      }
    }
    Ok(IndexerResponse {
      status: 200,
      payload: r#"{"data":{}}"#.into(),
      attestation: None,
    })
  }
}

#[tokio::test]
#[ignore = "Writes output to disk"]
async fn indexer_timeout_simulation() {
  create_dir("test-outputs");

  let (mut input_writers, inputs) = Inputs::new();
  input_writers
    .indexer_inputs
    .slashing_percentage
    .write("0.1".parse().unwrap());
  input_writers
    .indexer_inputs
    .usd_to_grt_conversion
    .write(1u64.try_into().unwrap());

  let network = "test";
  let test_key = SecretKey::from_str(TEST_KEY).unwrap();
  let blocks = gen_blocks(&(1..10).into_iter().collect::<Vec<u64>>());
  let block_resolver = BlockResolver::test(&blocks);
  let latest = blocks.last().unwrap();
  let deployment: SubgraphDeploymentID = bytes_from_id(1).into();

  let add_delay = Arc::new(Mutex::new(false));
  let indexer_ids = (1..=3)
    .map(|id| bytes_from_id(id).into())
    .collect::<Vec<Address>>();
  let sim_indexers = SimIndexers::new(
    [
      (indexer_ids[0].clone(), 0.0),
      (indexer_ids[1].clone(), 0.5),
      (indexer_ids[2].clone(), 1.0),
    ]
    .into_iter()
    .collect(),
    add_delay.clone(),
  );

  let indexings = indexer_ids.iter().map(|indexer| Indexing {
    indexer: indexer.clone(),
    deployment,
  });
  for indexing in indexings {
    let indexing_writer = input_writers
      .indexer_inputs
      .indexings
      .write(&indexing)
      .await;
    indexing_writer
      .cost_model
      .write(default_cost_model("0.00001".parse().unwrap()));
    indexing_writer
      .add_allocation(
        Address::default(),
        test_key.clone(),
        100_000u64.try_into().unwrap(),
      )
      .await;
    indexing_writer.status.write(IndexingStatus {
      block: latest.number,
      latest: latest.number,
    });
    let indexer_writer = input_writers
      .indexer_inputs
      .indexers
      .write(&indexing.indexer)
      .await;
    indexer_writer.url.write(Arc::default());
    indexer_writer.stake.write(100_000u64.try_into().unwrap());
  }

  input_writers.deployment_indexers.write(Ptr::new(
    [(deployment, indexer_ids.to_vec())].into_iter().collect(),
  ));

  let resolvers = [(network.to_string(), block_resolver)]
    .into_iter()
    .collect();
  let query_engine = Arc::new(QueryEngine::<_, _, FishermanClient>::new(
    Config {
      indexer_selection_retry_limit: 5,
      budget_factors: QueryBudgetFactors {
        scale: 1.0,
        discount: 0.0,
        processes: 1.0,
      },
    },
    sim_indexers.clone(),
    Arc::new(DummyKafka),
    None,
    Arc::new(resolvers),
    inputs.clone(),
  ));

  eventuals::idle().await;

  let query_subgraph = Some(Ptr::new(SubgraphInfo {
    deployment,
    network: network.to_string(),
    features: vec![],
    min_block: 0,
  }));
  let api_key = Some(Arc::new(APIKey {
    id: 0,
    key: "".into(),
    user_id: 0,
    user_address: bytes_from_id(0).into(),
    queries_activated: true,
    max_budget: None,
    deployments: vec![],
    subgraphs: vec![],
    domains: vec![],
    indexer_preferences: IndexerPreferences {
      economic_security: 0.0,
      performance: 0.0,
      data_freshness: 0.0,
      price_efficiency: 0.0,
    },
    usage: Arc::new(Mutex::new(VolumeEstimator::default())),
  }));

  let queries = 10_000;
  let qps = 20;
  let mut counts = indexer_ids
    .iter()
    .map(|id| (id.clone(), 0.0))
    .collect::<BTreeMap<Address, f64>>();
  let mut data = indexer_ids
    .iter()
    .map(|id| (id.clone(), vec![]))
    .collect::<BTreeMap<Address, Vec<(f64, f64)>>>();
  for i in 0..queries {
    // Warm up ISA for first 2,000 iterations
    if i == 2_000 {
      *add_delay.lock().await = true;
    }
    // Trigger decay every minute, simulating 20 queries per second.
    if (i % (qps * 60)) == 0 {
      inputs.indexers.decay().await;
    }
    let mut query = Query::new("".into(), "{_meta{block{number}}}".into(), None);
    query.subgraph = query_subgraph.clone();
    query.api_key = api_key.clone();
    if let Err(_) = query_engine.execute_query(&mut query).await {
      continue;
    }
    let selected = &query.indexer_attempts.last().unwrap().indexer;
    *counts.get_mut(selected).unwrap() += 1.0;
    let t = i as f64 / qps as f64;
    for (k, v) in data.iter_mut() {
      v.push((t, (*counts.get(k).unwrap() / (i + 1) as f64) * 100.0));
    }
  }

  let file_path = "test-outputs/simulated-timeouts.svg";
  let root = SVGBackend::new(&file_path, (1600, 800)).into_drawing_area();
  let mut plot = ChartBuilder::on(&root)
    .margin(5)
    .x_label_area_size(45)
    .y_label_area_size(35)
    .build_cartesian_2d(0.0..(queries as f64 / qps as f64), 0.0..100.0)
    .unwrap();
  plot
    .configure_mesh()
    .x_desc("time (s)")
    .y_desc("% selected")
    .draw()
    .unwrap();
  let colors = [1, 4, 0];
  let labels = sim_indexers
    .indexer_timeout_rates
    .values()
    .map(|timeout_rate| format!("{}% timeout", (timeout_rate * 100.0).round()));
  for (((_, points), color), label) in data.into_iter().zip(colors).zip(labels) {
    plot
      .draw_series(LineSeries::new(
        points,
        Palette99::pick(color).stroke_width(2),
      ))
      .unwrap()
      .label(label)
      .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &Palette99::pick(color)));
  }
  plot
    .configure_series_labels()
    .background_style(&WHITE.mix(0.8))
    .border_style(&BLACK)
    .draw()
    .unwrap();
}
