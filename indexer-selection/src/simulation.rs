use crate::{test_utils::default_cost_model, *};
use anyhow::Result;
use prelude::{
    test_utils::{bytes_from_id, init_test_tracing},
    *,
};
use rand::{prelude::SmallRng, Rng as _, SeedableRng as _};
use rand_distr::Normal;
use std::sync::Arc;

pub struct IndexerCharacteristics {
    pub address: Address,
    pub fee: GRT,
    pub blocks_behind: u64,
    pub latency_ms: u64,
    pub success_rate: f64,
    pub allocation: GRT,
    pub stake: GRT,
}

#[derive(Default)]
pub struct Results {
    pub selections: Vec<Selection>,
    pub client_queries: u64,
    pub success_rate: f64,
    pub avg_latency: f64,
    pub avg_blocks_behind: f64,
    pub avg_indexers_selected: f64,
    pub avg_selection_seconds: f64,
}

pub async fn simulate(
    characteristics: &[IndexerCharacteristics],
    params: &UtilityParameters,
    queries_per_second: u64,
    selection_limit: u8,
) -> Result<Results> {
    init_test_tracing();

    let deployment = DeploymentId(bytes_from_id(1));
    let mut results = simulation::Results {
        client_queries: 10_000,
        ..Default::default()
    };

    let mut isa = State::default();
    isa.network_params.slashing_percentage = "0.1".parse().ok();
    isa.network_params.usd_to_grt_conversion = "0.1".parse().ok();

    for characteristics in characteristics {
        let indexing = Indexing {
            indexer: characteristics.address,
            deployment,
        };
        isa.indexers.insert(
            indexing.indexer,
            Arc::new(IndexerInfo {
                url: "http://localhost".parse().unwrap(),
                stake: characteristics.stake,
            }),
        );
        let allocations = Arc::new(
            [(Address::default(), characteristics.allocation)]
                .into_iter()
                .collect(),
        );
        isa.insert_indexing(
            indexing,
            IndexingStatus {
                allocations,
                cost_model: Some(Ptr::new(default_cost_model(characteristics.fee))),
                block: Some(BlockStatus {
                    reported_number: params
                        .latest_block
                        .saturating_sub(characteristics.blocks_behind),
                    blocks_behind: characteristics.blocks_behind,
                    behind_reported_block: false,
                    min_block: None,
                }),
            },
        );
    }

    let indexings: Vec<Indexing> = characteristics
        .iter()
        .map(|c| Indexing {
            indexer: c.address,
            deployment,
        })
        .collect();
    let characteristics: HashMap<&Address, &IndexerCharacteristics> =
        characteristics.iter().map(|c| (&c.address, c)).collect();
    let mut rng = SmallRng::from_entropy();
    for client_query_index in 0..results.client_queries {
        if (client_query_index % queries_per_second) == 0 {
            isa.decay();
        }

        let mut context = Context::new("{ a }", "").unwrap();
        let t0 = Instant::now();
        let (mut selections, _) = isa
            .select_indexers(&indexings, params, &mut context, selection_limit)
            .unwrap();
        results.avg_selection_seconds += Instant::now().duration_since(t0).as_secs_f64();

        selections
            .sort_unstable_by_key(|s| characteristics.get(&s.indexing.indexer).unwrap().latency_ms);
        let responses = selections
            .iter()
            .map(|s| {
                let c = *characteristics.get(&s.indexing.indexer).unwrap();
                (c, c.success_rate > rng.gen())
            })
            .collect::<Vec<(&IndexerCharacteristics, bool)>>();
        let responding_indexer = responses.iter().filter(|(_, ok)| *ok).fuse().next();
        results.avg_indexers_selected += selections.len() as f64;
        if let Some((characteristics, _)) = responding_indexer {
            results.success_rate += 1.0;
            results.avg_blocks_behind += characteristics.blocks_behind as f64;
            let latency_distr = Normal::new(characteristics.latency_ms as f64, 50.0).unwrap();
            results.avg_latency += rng.sample(latency_distr).max(0.0);
        }

        results.selections.extend_from_slice(&selections);
        for (characteristics, ok) in responses {
            let indexing = Indexing {
                indexer: characteristics.address,
                deployment,
            };
            let duration = Duration::from_millis(characteristics.latency_ms);
            let result = match ok {
                true => Ok(()),
                false => Err(IndexerErrorObservation::Other),
            };
            isa.observe_query(&indexing, duration, result);
        }
    }

    results.success_rate /= results.client_queries as f64;
    results.avg_latency /= results.client_queries as f64;
    results.avg_blocks_behind /= results.client_queries as f64;
    results.avg_indexers_selected /= results.client_queries as f64;
    results.avg_selection_seconds /= results.client_queries as f64;

    Ok(results)
}
