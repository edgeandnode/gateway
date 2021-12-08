use crate::{
    block_resolver::BlockResolver,
    indexer_selection::{
        test_utils::{default_cost_model, gen_blocks, TEST_KEY},
        *,
    },
    prelude::{test_utils::*, *},
};
use rand::{thread_rng, Rng as _};
use secp256k1::SecretKey;
use std::collections::{BTreeMap, HashMap};

struct IndexerCharacteristics {
    stake: GRT,
    allocation: GRT,
    blocks_behind: u64,
    price: GRT,
    latency_ms: u64,
    reliability: f64,
}

#[derive(Debug, Default, Clone)]
struct IndexerResults {
    queries_received: i64,
    query_fees: GRT,
}

#[tokio::test]
async fn battle_high_and_low() {
    init_test_tracing();
    let (mut input_writers, inputs) = Indexers::inputs();
    let indexers = Indexers::new(inputs);
    input_writers
        .slashing_percentage
        .write("0.1".parse().unwrap());
    input_writers
        .usd_to_grt_conversion
        .write(1u64.try_into().unwrap());

    let network = "test";
    let blocks = gen_blocks(&(0u64..100).into_iter().collect::<Vec<u64>>());
    let resolver = BlockResolver::test(&blocks);
    let latest = blocks.last().unwrap();
    let deployment: SubgraphDeploymentID = bytes_from_id(99).into();
    let tests = [
        // Great!
        IndexerCharacteristics {
            stake: 500000u64.try_into().unwrap(),
            allocation: 600000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 80,
            reliability: 0.999,
            blocks_behind: 0,
        },
        // Also great!
        IndexerCharacteristics {
            stake: 400000u64.try_into().unwrap(),
            allocation: 800000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 70,
            reliability: 0.995,
            blocks_behind: 1,
        },
        // Ok!
        IndexerCharacteristics {
            stake: 300000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000034".parse().unwrap(),
            latency_ms: 130,
            reliability: 0.95,
            blocks_behind: 1,
        },
        // Race to the bottom
        IndexerCharacteristics {
            stake: 400000u64.try_into().unwrap(),
            allocation: 40000u64.try_into().unwrap(),
            price: "0.000005".parse().unwrap(),
            latency_ms: 250,
            reliability: 0.96,
            blocks_behind: 1,
        },
        // Meh
        IndexerCharacteristics {
            stake: 100000u64.try_into().unwrap(),
            allocation: 100000u64.try_into().unwrap(),
            price: "0.000024".parse().unwrap(),
            latency_ms: 200,
            reliability: 0.80,
            blocks_behind: 8,
        },
        // Bad
        IndexerCharacteristics {
            stake: 100000u64.try_into().unwrap(),
            allocation: 100000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 1900,
            reliability: 0.80,
            blocks_behind: 8,
        },
        // Overpriced
        IndexerCharacteristics {
            stake: 500000u64.try_into().unwrap(),
            allocation: 600000u64.try_into().unwrap(),
            price: "0.000999".parse().unwrap(),
            latency_ms: 80,
            reliability: 0.999,
            blocks_behind: 0,
        },
        // Optimize economic security
        IndexerCharacteristics {
            stake: 1000000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000045".parse().unwrap(),
            latency_ms: 120,
            reliability: 0.99,
            blocks_behind: 1,
        },
        // Optimize performance
        IndexerCharacteristics {
            stake: 400000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 60,
            reliability: 0.99,
            blocks_behind: 1,
        },
        // Optimize reliability
        IndexerCharacteristics {
            stake: 300000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000035".parse().unwrap(),
            latency_ms: 120,
            reliability: 0.999,
            blocks_behind: 0,
        },
    ];

    let mut data = HashMap::new();
    let mut indexer_ids = im::Vector::new();
    let test_key = SecretKey::from_str(TEST_KEY).unwrap();
    for indexer in tests.iter() {
        let indexing = Indexing {
            indexer: bytes_from_id(indexer_ids.len()).into(),
            deployment,
        };
        let indexing_writer = input_writers.indexings.write(&indexing).await;
        indexing_writer
            .cost_model
            .write(default_cost_model(indexer.price));
        indexing_writer
            .add_allocation(Address::default(), test_key.clone(), indexer.allocation)
            .await;
        indexing_writer.status.write(IndexingStatus {
            block: latest.number - indexer.blocks_behind,
            latest: latest.number,
        });
        let indexer_writer = input_writers.indexers.write(&indexing.indexer).await;
        indexer_writer.url.write("".to_string());
        indexer_writer.stake.write(indexer.stake);
        data.insert(indexing.indexer, indexer);
        indexer_ids.push_back(indexing.indexer);
    }

    eventuals::idle().await;

    let config = UtilityConfig::default();
    let mut results = BTreeMap::<Address, IndexerResults>::new();

    let query_time = Instant::now();
    const COUNT: usize = 86400;
    const QPS: u64 = 2000;
    for i in 0..COUNT {
        let budget: GRT = "0.00005".parse().unwrap();
        let mut context = Context::new("{ a }", "").unwrap();
        let freshness_requirements = Indexers::freshness_requirements(&mut context, &resolver)
            .await
            .unwrap();
        let result = indexers
            .select_indexer(
                &config,
                network,
                &deployment,
                &indexer_ids,
                &mut context,
                &resolver,
                &freshness_requirements,
                budget,
            )
            .await
            .unwrap();

        // This will make almost no difference.
        // Just testing.
        if i % (COUNT / 10) == 0 {
            indexers.decay().await;
        }

        let query = match result {
            Some(query) => query,
            None => continue,
        };
        let entry = results.entry(query.indexing.indexer).or_default();
        entry.queries_received += 1;
        let data = data.get(&query.indexing.indexer).unwrap();
        let indexing = Indexing {
            deployment,
            indexer: query.indexing.indexer,
        };
        if data.reliability > thread_rng().gen() {
            let duration = Duration::from_millis(data.latency_ms);
            let receipt = &query.receipt;
            let fees: GRTWei =
                primitive_types::U256::from_big_endian(&receipt[(receipt.len() - 32)..])
                    .try_into()
                    .unwrap();
            entry.query_fees = fees.shift();
            indexers
                .observe_successful_query(&indexing, duration, &query.receipt)
                .await;
        } else {
            indexers
                .observe_failed_query(
                    &indexing,
                    &query.receipt,
                    IndexerError::Other("error".to_string()),
                )
                .await;
        }
    }

    let query_time = Instant::now() - query_time;
    println!("Thoughput: {} /s", COUNT as f64 / query_time.as_secs_f64());
    let columns = vec![
        "ID",
        "Stake",
        "Allocation",
        "Blocks Behind",
        "Price",
        "Latency",
        "Reliability",
        "Daily Fees",
        "Queries Served",
    ];
    println!("| {} |", columns.join(" | "));
    println!(
        "| {}",
        std::iter::repeat("--- |")
            .take(columns.len())
            .collect::<String>()
    );

    let mut total_fees = GRT::zero();
    for (name, indexer_id) in indexer_ids.iter().enumerate() {
        let data = data.get(indexer_id).unwrap();
        let results = results.get(indexer_id).cloned().unwrap_or_default();

        println!(
            "| {} | {} GRT | {} GRT | {} | {} USD | {} ms | {}% | {} USD | {:.1}% |",
            name,
            data.stake,
            data.allocation,
            data.blocks_behind,
            data.price,
            data.latency_ms,
            data.reliability * 100.0,
            (results.query_fees * QPS.try_into().unwrap()),
            (results.queries_received * 100) as f64 / COUNT as f64,
        );

        total_fees += results.query_fees;
    }
    println!("Total Fees: {}", (total_fees * QPS.try_into().unwrap()));
}
