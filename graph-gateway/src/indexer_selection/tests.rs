use crate::{
    block_resolver::BlockResolver,
    indexer_selection::{
        test_utils::{default_cost_model, gen_blocks, TEST_KEY},
        *,
    },
};
use plotters::{
    prelude::{DrawingBackend, *},
    style,
};
use prelude::{test_utils::*, *};
use rand::{thread_rng, Rng as _};
use secp256k1::SecretKey;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
struct IndexerCharacteristics {
    label: &'static str,
    stake: GRT,
    allocation: GRT,
    blocks_behind: u64,
    price: GRT,
    latency_ms: u64,
    reliability: f64,
    special_weight: Option<f64>,
}

#[derive(Debug, Default, Clone)]
struct IndexerResults {
    queries_received: i64,
    query_fees: GRT,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Writes output to disk"]
async fn weights() {
    init_test_tracing();

    let tests = [
        IndexerCharacteristics {
            label: "Overpriced",
            stake: 500000u64.try_into().unwrap(),
            allocation: 600000u64.try_into().unwrap(),
            price: "0.000999".parse().unwrap(),
            latency_ms: 80,
            reliability: 0.999,
            blocks_behind: 0,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Great!",
            stake: 800000u64.try_into().unwrap(),
            allocation: 600000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 80,
            reliability: 0.999,
            blocks_behind: 0,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Also great!",
            stake: 400000u64.try_into().unwrap(),
            allocation: 800000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 70,
            reliability: 0.995,
            blocks_behind: 1,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Ok!",
            stake: 300000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000034".parse().unwrap(),
            latency_ms: 130,
            reliability: 0.95,
            blocks_behind: 2,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Race to the bottom",
            stake: 400000u64.try_into().unwrap(),
            allocation: 40000u64.try_into().unwrap(),
            price: "0.000005".parse().unwrap(),
            latency_ms: 250,
            reliability: 0.96,
            blocks_behind: 3,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Meh",
            stake: 100000u64.try_into().unwrap(),
            allocation: 100000u64.try_into().unwrap(),
            price: "0.000024".parse().unwrap(),
            latency_ms: 200,
            reliability: 0.80,
            blocks_behind: 8,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Bad",
            stake: 100000u64.try_into().unwrap(),
            allocation: 100000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 1900,
            reliability: 0.80,
            blocks_behind: 8,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Optimize economic security",
            stake: 2000000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000045".parse().unwrap(),
            latency_ms: 120,
            reliability: 0.99,
            blocks_behind: 2,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Optimize performance",
            stake: 400000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000040".parse().unwrap(),
            latency_ms: 60,
            reliability: 0.95,
            blocks_behind: 1,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Optimize reliability",
            stake: 300000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.000035".parse().unwrap(),
            latency_ms: 120,
            reliability: 0.999,
            blocks_behind: 4,
            special_weight: None,
        },
        IndexerCharacteristics {
            label: "Backstop",
            stake: 300000u64.try_into().unwrap(),
            allocation: 400000u64.try_into().unwrap(),
            price: "0.00003".parse().unwrap(),
            latency_ms: 70,
            reliability: 0.999,
            blocks_behind: 1,
            special_weight: Some(0.2),
        },
    ];

    let columns = vec![
        "ID",
        "Stake",
        "Allocation",
        "Blocks Behind",
        "Price",
        "Latency",
        "Reliability",
        "Special Weight",
    ];
    println!("| {} |", columns.join(" | "));
    println!(
        "| {}",
        std::iter::repeat("--- |")
            .take(columns.len())
            .collect::<String>()
    );
    for test in &tests {
        println!(
            "| {} | {} GRT | {} GRT | {} | {} USD | {} ms | {}% | {:?} |",
            test.label,
            test.stake,
            test.allocation,
            test.blocks_behind,
            test.price,
            test.latency_ms,
            test.reliability * 100.0,
            test.special_weight,
        );
    }

    use futures::{stream::FuturesOrdered, StreamExt as _};
    let results = vec![
        (0.0, 0.0, 0.0, 0.0),
        (1.0, 0.0, 0.0, 0.0),
        (0.0, 1.0, 0.0, 0.0),
        (0.0, 0.0, 1.0, 0.0),
        (0.0, 0.0, 0.0, 1.0),
    ]
    .into_iter()
    .map(|cfg| {
        let tests = tests.clone();
        async move {
            let config = UtilityConfig::from_preferences(&IndexerPreferences {
                economic_security: cfg.0,
                performance: cfg.1,
                data_freshness: cfg.2,
                price_efficiency: cfg.3,
            });
            run_simulation(&tests, &config).await
        }
    })
    .collect::<FuturesOrdered<_>>()
    .collect::<Vec<_>>()
    .await;
    let data = vec![
        ("economic_security 0.0", results[0].clone()),
        ("economic_security 1.0", results[1].clone()),
        ("performance 0.0", results[0].clone()),
        ("performance 1.0", results[2].clone()),
        ("data_freshness 0.0", results[0].clone()),
        ("data_freshness 1.0", results[3].clone()),
        ("price_efficiency 0.0", results[0].clone()),
        ("price_efficiency 1.0", results[4].clone()),
    ];
    let labels = tests
        .iter()
        .map(|data| data.label)
        .collect::<Vec<&'static str>>();
    visualize_outcomes(data, 2, &labels);
}

async fn run_simulation(
    tests: &[IndexerCharacteristics],
    utility_config: &UtilityConfig,
) -> Vec<f64> {
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

    let mut results = Vec::<IndexerResults>::new();
    let mut indexer_ids = Vec::new();
    let test_key = SecretKey::from_str(TEST_KEY).unwrap();
    let mut special_indexers = HashMap::<Address, NotNan<f64>>::new();
    for data in tests.iter() {
        results.push(IndexerResults::default());
        let indexing = Indexing {
            indexer: bytes_from_id(indexer_ids.len()).into(),
            deployment,
        };
        indexer_ids.push(indexing.indexer);
        let indexing_writer = input_writers.indexings.write(&indexing).await;
        indexing_writer
            .update_allocations(
                test_key.clone(),
                vec![(Address::default(), data.allocation)],
            )
            .await;
        indexing_writer.status.write(IndexingStatus {
            cost_model: Some(Ptr::new(default_cost_model(data.price))),
            block: latest.number - data.blocks_behind,
            latest: latest.number,
        });
        let indexer_writer = input_writers.indexers.write(&indexing.indexer).await;
        indexer_writer
            .url
            .write(Arc::new("http://localhost".parse().unwrap()));
        indexer_writer.stake.write(data.stake);
        if let Some(special_weight) = data.special_weight {
            special_indexers.insert(indexing.indexer, special_weight.try_into().unwrap());
        }
    }

    input_writers.special_indexers.write(special_indexers);

    eventuals::idle().await;

    const COUNT: usize = 86400;
    const QPS: u64 = 2000;
    let mut total_latency_ms = 0;
    let mut total_blocks_behind = 0;
    for i in 0..COUNT {
        let budget: GRT = "0.00005".parse().unwrap();
        let mut context = Context::new("{ a }", "").unwrap();
        let freshness_requirements = Indexers::freshness_requirements(&mut context, &resolver)
            .await
            .unwrap();
        let result = indexers
            .select_indexer(
                &utility_config,
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

        if i % (COUNT / QPS as usize) == 0 {
            indexers.decay().await;
        }

        let query = match result {
            Some((query, _)) => query,
            None => continue,
        };
        let index = indexer_ids
            .iter()
            .position(|id| id == &query.indexing.indexer)
            .unwrap();
        let entry = results.get_mut(index).unwrap();
        entry.queries_received += 1;
        let data = tests.get(index).unwrap();
        let indexing = Indexing {
            deployment,
            indexer: query.indexing.indexer,
        };
        let duration = Duration::from_millis(data.latency_ms);
        if data.reliability > thread_rng().gen() {
            total_latency_ms += data.latency_ms;
            total_blocks_behind += data.blocks_behind;
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
                    duration,
                    &query.receipt,
                    &IndexerError::Other("error".to_string()),
                )
                .await;
        }
    }

    let mut total_fees = GRT::zero();
    for result in &results {
        total_fees += result.query_fees;
    }
    println!(
        "{:?}, fees: {}, avg. latency: {}ms, avg. blocks behind: {}",
        utility_config,
        total_fees,
        total_latency_ms / COUNT as u64,
        total_blocks_behind / COUNT as u64,
    );
    results
        .iter()
        .map(|result| (result.query_fees / total_fees).as_f64())
        .collect::<Vec<f64>>()
}

fn visualize_outcomes(data: Vec<(&str, Vec<f64>)>, columns: usize, labels: &[&'static str]) {
    create_dir("test-outputs");
    let output_file = "test-outputs/isa-weights.png";

    let mut root = BitMapBackend::new(output_file, (2100, 480));
    let text_style = TextStyle::from(("sans-serif", 28.0).into_font());
    // Fill background
    let (width, height) = root.get_size();
    root.draw_rect((0, 0), (width as i32, height as i32), &WHITE, true)
        .unwrap();
    // Add legend
    {
        let x = 1750;
        let y = 10;
        for (i, label) in labels.iter().enumerate().map(|(i, l)| (i as i32, l)) {
            let style = style::Palette99::pick(i as usize).to_rgba();
            root.draw_text(label, &text_style, (x + 20, y + 40 * i))
                .unwrap();
            let color_pos = ((x, y + 40 * i), (x + 15, y + 40 * i + 15));
            root.draw_rect(color_pos.0, color_pos.1, &style, true)
                .unwrap();
            root.draw_rect(color_pos.0, color_pos.1, &BLACK, false)
                .unwrap();
        }
    }
    // Add bars
    for (i, (label, data)) in data.iter().enumerate().map(|(i, d)| (i as i32, d)) {
        let x = 440 * (i / columns as i32);
        let y = 150 * (i % columns as i32);
        draw_bar(&mut root, (x + 10, y + 50), (x + 400, y + 150), &data);
        root.draw_text(&label, &text_style, (x + 20, y + 16))
            .unwrap();
    }
    root.present().unwrap();
}

fn draw_bar<B: DrawingBackend>(
    root: &mut B,
    upper_left: (i32, i32),
    bottom_right: (i32, i32),
    data: &[f64],
) {
    let len = (bottom_right.0 - upper_left.0) as f64;
    let mut start = upper_left.0;
    for (i, fraction) in data.iter().enumerate() {
        let style = style::Palette99::pick(i).to_rgba();
        let end = start + (len * fraction) as i32;
        root.draw_rect((start, upper_left.1), (end, bottom_right.1), &style, true)
            .unwrap();
        root.draw_rect((start, upper_left.1), (end, bottom_right.1), &BLACK, false)
            .unwrap();
        start = end;
    }
    // Pad rounding errors
    root.draw_rect((start, upper_left.1), bottom_right, &BLACK, true)
        .unwrap();
    // Draw outline
    root.draw_rect(upper_left, bottom_right, &BLACK, false)
        .unwrap();
}
