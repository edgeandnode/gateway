use crate::{
    indexing::BlockStatus,
    test_utils::{default_cost_model, gen_blocks},
    *,
};
use plotters::{
    prelude::{DrawingBackend, *},
    style,
};
use prelude::{test_utils::*, *};
use rand::{thread_rng, Rng as _};
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
    let mut isa = State::default();
    isa.network_params.slashing_percentage = "0.1".parse().ok();
    isa.network_params.usd_to_grt_conversion = 1u64.try_into().ok();

    let blocks = gen_blocks(&(0u64..100).into_iter().collect::<Vec<u64>>());
    let latest = blocks.last().unwrap();
    let deployment: SubgraphDeploymentID = bytes_from_id(99).into();

    let mut indexer_ids = Vec::new();
    let mut special_indexers = HashMap::<Address, NotNan<f64>>::new();
    for data in tests.iter() {
        let indexing = Indexing {
            indexer: bytes_from_id(indexer_ids.len()).into(),
            deployment,
        };
        isa.indexers.insert(
            indexing.indexer,
            Arc::new(IndexerInfo {
                url: "http://localhost".parse().unwrap(),
                stake: data.stake,
            }),
        );
        let allocations = Arc::new(
            [(Address::default(), data.allocation)]
                .into_iter()
                .collect(),
        );
        isa.insert_indexing(
            indexing,
            IndexingStatus {
                allocations,
                cost_model: Some(Ptr::new(default_cost_model(data.price))),
                block: Some(BlockStatus {
                    reported_number: latest.number.saturating_sub(data.blocks_behind),
                    blocks_behind: data.blocks_behind,
                    behind_reported_block: false,
                }),
            },
        );
        indexer_ids.push(indexing.indexer);
        if let Some(special_weight) = data.special_weight {
            special_indexers.insert(indexing.indexer, special_weight.try_into().unwrap());
        }
    }

    isa.special_indexers = Some(Arc::new(special_indexers));

    const COUNT: usize = 86400;
    const QPS: u64 = 2000;
    let mut total_latency_ms = 0;
    let mut total_blocks_behind = 0;
    let mut total_indexers_selected = 0;
    let mut results = tests.iter().map(|_| GRT::zero()).collect::<Vec<GRT>>();
    for i in 0..COUNT {
        let budget: GRT = "0.0001".parse().unwrap();
        let mut context = Context::new("{ a }", "").unwrap();
        let freshness_requirements = FreshnessRequirements {
            minimum_block: None,
            has_latest: true,
        };
        let latest_block = blocks.last().unwrap().number;
        let (mut selections, _) = isa
            .select_indexers(
                &deployment,
                &indexer_ids,
                &utility_config,
                &freshness_requirements,
                &mut context,
                latest_block,
                budget,
                5,
            )
            .unwrap();

        if i % (COUNT / QPS as usize) == 0 {
            isa.decay();
        }

        total_indexers_selected += selections.len() as u64;

        let data = selections
            .iter()
            .map(|s| {
                let indexer = s.indexing.indexer.clone();
                let i = indexer_ids.iter().position(|id| id == &indexer).unwrap();
                (indexer, i)
            })
            .collect::<HashMap<Address, usize>>();
        selections.sort_by(|a, b| {
            let a = tests[*data.get(&a.indexing.indexer).unwrap()].latency_ms;
            let b = tests[*data.get(&b.indexing.indexer).unwrap()].latency_ms;
            a.cmp(&b)
        });
        let responding_indexer = selections
            .iter()
            .filter_map(|s| {
                let characteristics = &tests[*data.get(&s.indexing.indexer).unwrap()];
                if characteristics.reliability > thread_rng().gen() {
                    Some(characteristics)
                } else {
                    None
                }
            })
            .fuse()
            .next();
        if let Some(characteristics) = responding_indexer {
            total_latency_ms += characteristics.latency_ms;
            total_blocks_behind += characteristics.blocks_behind;
        }

        for selection in selections {
            let index = *data.get(&selection.indexing.indexer).unwrap();
            results[index] += selection.fee;
            let characteristics = tests.get(index).unwrap();
            let indexing = Indexing {
                deployment,
                indexer: selection.indexing.indexer,
            };
            let duration = Duration::from_millis(characteristics.latency_ms);
            if characteristics.reliability > thread_rng().gen() {
                isa.observe_successful_query(&indexing, duration);
            } else {
                isa.observe_failed_query(&indexing, duration, false);
            }
        }
    }

    let total_fees = results.iter().fold(GRT::zero(), |sum, fees| sum + *fees);
    println!(
        "{:?}, fees: {}, avg. latency: {}ms, avg. blocks behind: {}, avg. indexers selected: {}",
        utility_config,
        total_fees,
        total_latency_ms / COUNT as u64,
        total_blocks_behind / COUNT as u64,
        total_indexers_selected / COUNT as u64,
    );
    results
        .iter()
        .map(|fees| (*fees / total_fees).as_f64())
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
