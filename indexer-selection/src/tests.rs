use crate::{
    indexing::BlockStatus,
    test_utils::{default_cost_model, gen_blocks},
    *,
};
use prelude::{test_utils::*, *};
use rand::{thread_rng, Rng as _};
use std::{collections::HashMap, io::Write as _, sync::Arc};

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

#[tokio::test]
#[ignore = "Writes output to disk"]
async fn simulation() {
    let indexers = [
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
    for indexer in &indexers {
        println!(
            "| {} | {} GRT | {} GRT | {} | {} USD | {} ms | {}% | {:?} |",
            indexer.label,
            indexer.stake,
            indexer.allocation,
            indexer.blocks_behind,
            indexer.price,
            indexer.latency_ms,
            indexer.reliability * 100.0,
            indexer.special_weight,
        );
    }

    let config = UtilityConfig::from_preferences(&IndexerPreferences {
        economic_security: 0.0,
        performance: 0.0,
        data_freshness: 0.0,
        price_efficiency: 0.0,
    });
    run_simulation(&indexers, &config).await
}

async fn run_simulation(indexers: &[IndexerCharacteristics], utility_config: &UtilityConfig) {
    #[derive(Default)]
    struct IndexerResult {
        total_selections: u64,
        total_fees: GRT,
    }

    let out = create_test_output("simulation.csv").unwrap();
    writeln!(&out, "indexer,fees,selections").unwrap();

    let mut isa = State::default();
    isa.network_params.slashing_percentage = "0.1".parse().ok();
    isa.network_params.usd_to_grt_conversion = 1u64.try_into().ok();

    let blocks = gen_blocks(&(0u64..100).into_iter().collect::<Vec<u64>>());
    let latest = blocks.last().unwrap();
    let deployment: SubgraphDeploymentID = bytes_from_id(99).into();

    let mut indexer_ids = Vec::new();
    let mut special_indexers = HashMap::<Address, NotNan<f64>>::new();
    for data in indexers {
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
    let mut results = indexers
        .iter()
        .map(|_| IndexerResult::default())
        .collect::<Vec<IndexerResult>>();
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
        selections.sort_by_key(|s| indexers[*data.get(&s.indexing.indexer).unwrap()].latency_ms);
        let responding_indexer = selections
            .iter()
            .filter_map(|s| {
                let characteristics = &indexers[*data.get(&s.indexing.indexer).unwrap()];
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
            results[index].total_selections += 1;
            results[index].total_fees += selection.fee;
            let characteristics = indexers.get(index).unwrap();
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

    for (result, indexer) in results.iter().zip(indexers) {
        writeln!(
            &out,
            "{},{},{}",
            indexer.label, result.total_fees, result.total_selections
        )
        .unwrap();
    }

    let total_fees = results
        .iter()
        .fold(GRT::zero(), |sum, r| sum + r.total_fees);
    println!("{:?}", utility_config);
    println!(
        "total fees: {:.6}, avg. latency: {} ms, avg. blocks behind: {}, avg. indexers selected: {}",
        total_fees,
        total_latency_ms / COUNT as u64,
        total_blocks_behind / COUNT as u64,
        total_indexers_selected / COUNT as u64,
    );
}
