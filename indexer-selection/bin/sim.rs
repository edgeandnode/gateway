use std::io::{stdin, BufRead as _};

use anyhow::Result;

use gateway_common::utils::testing::gen_blocks;
use indexer_selection::{
    simulation::*, tokens::GRT, BlockRequirements, Selection, UtilityParameters,
};
use thegraph::types::UDecimal18;

#[tokio::main]
async fn main() -> Result<()> {
    let header = "indexer,fee,blocks_behind,latency_ms,success_rate,allocation,stake";
    let characteristics = stdin()
        .lock()
        .lines()
        .filter_map(|line| {
            let line = line.unwrap();
            if line == header {
                return None;
            }
            let fields = line.split(',').collect::<Vec<&str>>();
            Some(IndexerCharacteristics {
                address: fields[0].parse().expect("address"),
                fee: GRT(fields[1].parse().expect("fee")),
                blocks_behind: fields[2].parse::<f64>().expect("blocks_behind").round() as u64,
                latency_ms: fields[3].parse::<f64>().expect("latency_ms").round() as u64,
                success_rate: fields[4].parse().expect("success_rate"),
                allocation: GRT(fields[5].parse().expect("allocation")),
                stake: GRT(fields[6].parse().expect("stake")),
            })
        })
        .collect::<Vec<IndexerCharacteristics>>();

    let budget = GRT(UDecimal18::try_from(0.01).unwrap());
    let freshness_requirements = BlockRequirements {
        range: None,
        has_latest: true,
    };
    let blocks = {
        let max_blocks_behind = characteristics.iter().map(|c| c.blocks_behind).max();
        let last_block = max_blocks_behind.unwrap() + 100;
        gen_blocks(&(0..last_block).collect::<Vec<u64>>())
    };
    let latest_block = blocks.last().unwrap().number;
    let params = UtilityParameters {
        budget,
        requirements: freshness_requirements,
        latest_block,
        block_rate_hz: 0.1,
    };

    println!("label,indexer,detail,selections,fees");
    eprintln!("| selection limit | total fees (GRT) | avg. latency (ms) | avg. blocks behind | avg. indexers selected | avg. selection time (ms) |");
    eprintln!("| --- | --- | --- | --- | --- | --- |");
    for selection_limit in [1, 3] {
        let results = simulate(&characteristics, &params, 100, selection_limit).await?;

        let total_cost = results.selections.iter().map(|s| s.fee).sum::<u128>() as f64 * 1e-18;
        eprintln!(
            "| {} | {:.6} | {:.0} | {:.2} | {:.2} | {:.2} |",
            selection_limit,
            total_cost,
            results.avg_latency,
            results.avg_blocks_behind,
            results.selections.len() as f64 / results.client_queries as f64,
            results.avg_selection_seconds * 1e3,
        );

        for indexer in &characteristics {
            let selections = results
                .selections
                .iter()
                .filter(|s| s.indexing.indexer == indexer.address)
                .collect::<Vec<&Selection>>();
            let detail = format!(
                "fee={:.4} behind={:02} latency={:04} success={:.3} alloc={:1.0e} stake={:1.0e}",
                f64::from(indexer.fee.0),
                indexer.blocks_behind,
                indexer.latency_ms,
                indexer.success_rate,
                f64::from(indexer.allocation.0),
                f64::from(indexer.stake.0),
            );
            println!(
                "selection_limit={},{},{},{},{}",
                selection_limit,
                indexer.address,
                detail,
                selections.len(),
                selections.iter().map(|s| s.fee).sum::<u128>() as f64 * 1e-18,
            );
        }
    }

    Ok(())
}
