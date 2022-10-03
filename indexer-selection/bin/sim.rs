use anyhow::Result;
use indexer_selection::{simulation::*, Selection, UtilityConfig};
use prelude::*;
use std::io::{stdin, BufRead as _};

#[tokio::main]
async fn main() -> Result<()> {
    let header = "indexer,fee,blocks_behind,latency_ms,success_rate,allocation,stake";
    let characteristics = stdin()
        .lock()
        .lines()
        .filter_map(|line| {
            let line = line.unwrap();
            if &line == header {
                return None;
            }
            let fields = line.split(',').collect::<Vec<&str>>();
            Some(IndexerCharacteristics {
                address: fields[0].parse().expect("address"),
                fee: fields[1].parse().expect("fee"),
                blocks_behind: fields[2].parse::<f64>().expect("blocks_behind").round() as u64,
                latency_ms: fields[3].parse::<f64>().expect("latency_ms").round() as u64,
                success_rate: fields[4].parse().expect("success_rate"),
                allocation: fields[5].parse().expect("allocation"),
                stake: fields[6].parse().expect("stake"),
            })
        })
        .collect::<Vec<IndexerCharacteristics>>();

    let config = UtilityConfig::default();
    let budget = "0.001".parse().unwrap();

    println!("label,indexer,detail,selections,fees");
    eprintln!("| selection limit | total fees (GRT) | avg. latency (ms) | avg. blocks behind | avg. indexers selected |");
    eprintln!("| --- | --- | --- | --- | --- |");
    for selection_limit in [1, 3] {
        let results = simulate(&characteristics, &config, 100, budget, selection_limit).await?;

        let total_fees = results
            .selections
            .iter()
            .fold(GRT::zero(), |sum, s| sum + s.score.fee);
        eprintln!(
            "| {} | {:.6} | {:.0} | {:.2} | {:.2} |",
            selection_limit,
            total_fees,
            results.avg_latency,
            results.avg_blocks_behind,
            results.selections.len() as f64 / results.client_queries as f64,
        );

        for indexer in &characteristics {
            let selections = results
                .selections
                .iter()
                .filter(|s| s.indexing.indexer == indexer.address)
                .collect::<Vec<&Selection>>();
            let fees = selections
                .iter()
                .fold(GRT::zero(), |sum, s| sum + s.score.fee);
            let detail = format!(
                "fee={:.4} behind={:02} latency={:04} success={:.3} alloc={:1.0e} stake={:1.0e}",
                indexer.fee.as_f64(),
                indexer.blocks_behind,
                indexer.latency_ms,
                indexer.success_rate,
                indexer.allocation.as_f64(),
                indexer.stake.as_f64(),
            );
            println!(
                "selection_limit={},{},{},{},{}",
                selection_limit,
                indexer.address,
                detail,
                selections.len(),
                fees
            );
        }
    }

    Ok(())
}
