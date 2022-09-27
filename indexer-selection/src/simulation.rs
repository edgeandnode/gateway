use crate::{
    test_utils::{default_cost_model, gen_blocks},
    *,
};
use anyhow::Result;
use prelude::{test_utils::bytes_from_id, *};
use rand::prelude::SmallRng;
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
}

pub async fn simulate(
    characteristics: &[IndexerCharacteristics],
    config: &UtilityConfig,
    queries_per_second: u64,
    budget: GRT,
    selection_limit: u8,
) -> Result<Results> {
    let deployment = SubgraphDeploymentID(bytes_from_id(1));
    let mut results = Results::default();
    results.client_queries = 1000;

    let mut isa = State::default();
    isa.network_params.slashing_percentage = "0.1".parse().ok();
    isa.network_params.usd_to_grt_conversion = "0.1".parse().ok();

    let blocks = {
        let max_blocks_behind = characteristics.iter().map(|c| c.blocks_behind).max();
        let last_block = max_blocks_behind.unwrap() + 100;
        gen_blocks(&(0..last_block).into_iter().collect::<Vec<u64>>())
    };
    let latest_block = blocks.last().unwrap();

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
                    reported_number: latest_block
                        .number
                        .saturating_sub(characteristics.blocks_behind),
                    blocks_behind: characteristics.blocks_behind,
                    behind_reported_block: false,
                }),
            },
        );
    }

    let indexers = characteristics
        .iter()
        .map(|c| c.address)
        .collect::<Vec<Address>>();
    let characteristics = characteristics
        .iter()
        .map(|c| (&c.address, c))
        .collect::<HashMap<&Address, &IndexerCharacteristics>>();
    let mut rng = SmallRng::from_entropy();
    for client_query_index in 0..results.client_queries {
        if (client_query_index % queries_per_second) == 0 {
            isa.decay();
        }

        let mut context = Context::new("{ a }", "").unwrap();
        let freshness_requirements = FreshnessRequirements {
            minimum_block: None,
            has_latest: true,
        };
        let latest_block = blocks.last().unwrap().number;
        let (mut selections, _) = isa
            .select_indexers(
                &deployment,
                &indexers,
                config,
                &freshness_requirements,
                &mut context,
                latest_block,
                budget,
                selection_limit,
            )
            .unwrap();

        selections.sort_by_key(|s| characteristics.get(&s.indexing.indexer).unwrap().latency_ms);
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
            results.avg_latency += characteristics.latency_ms as f64;
            results.avg_blocks_behind += characteristics.blocks_behind as f64;
        }

        results.selections.extend_from_slice(&selections);
        for (characteristics, ok) in responses {
            let indexing = Indexing {
                indexer: characteristics.address,
                deployment,
            };
            let duration = Duration::from_millis(characteristics.latency_ms);
            if ok {
                isa.observe_successful_query(&indexing, duration);
            } else {
                isa.observe_failed_query(&indexing, duration, false);
            }
        }
    }

    results.success_rate /= results.client_queries as f64;
    results.avg_latency /= results.client_queries as f64;
    results.avg_blocks_behind /= results.client_queries as f64;
    results.avg_indexers_selected /= results.client_queries as f64;

    Ok(results)
}
