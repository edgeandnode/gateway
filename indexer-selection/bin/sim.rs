use chrono::{DateTime, FixedOffset};
use indexer_selection::{
    actor::{apply_state_update, IndexerUpdate, Update},
    test_utils::{default_cost_model, gen_blocks, test_allocation_id},
    BlockStatus, Context, FreshnessRequirements, IndexerInfo, IndexingStatus, State, UtilityConfig,
};
use itertools::Itertools as _;
use prelude::{graphql, *};
use rand::{rngs::SmallRng, seq::SliceRandom as _, Rng, SeedableRng};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashMap,
    fs,
    io::{BufRead as _, BufReader},
    sync::Arc,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let input_file = std::env::args()
        .nth(1)
        .ok_or(anyhow::anyhow!("missing input file"))?;
    let logs = BufReader::new(fs::File::open(input_file)?);
    let mut log_lines = logs
        .lines()
        .filter_map(|line| parse_log_line(line.as_ref().ok()?))
        .collect::<Vec<LogLine>>();
    log_lines.sort_by_key(|l| l.timestamp);

    let deployments = log_lines
        .iter()
        .map(|l| &l.deployment)
        .unique()
        .cloned()
        .collect::<Vec<SubgraphDeploymentID>>();
    if deployments.len() > 1 {
        eprintln!("WARNING: multiple deployments found!");
    }
    let deployment = deployments.first().cloned().unwrap();

    println!("label,timestamp,indexer,url,success,fee,blocks_behind,response_time_ms,utility");
    for line in &log_lines {
        println!(
            "{},{},{},{},{},{},{},{},{}",
            "example",
            line.timestamp,
            line.indexer,
            line.url,
            line.success,
            line.fee,
            line.blocks_behind,
            line.response_time_ms,
            line.utility,
        );
    }

    eprintln!("determining indexer characteristics...");

    let allocations = log_lines
        .iter()
        .map(|l| &l.allocation)
        .collect::<Vec<&String>>();

    let client = reqwest::Client::new();
    let allocations = graphql::query::<AllocationResponse>(
        &client,
        "https://gateway.thegraph.com/network".parse().unwrap(),
        &json!({
            "query": r#"query($allocations: [String!]!) {
            allocations(where:{id_in:$allocations}) {
                id
                allocatedTokens
                indexer { stakedTokens }
            }
        }"#,
            "variables": { "allocations": allocations },
        }),
    )
    .await
    .and_then(|r| r.unpack())
    .map_err(|err| anyhow::anyhow!(err))?
    .allocations;

    let indexers = log_lines
        .iter()
        .map(|l| &l.indexer)
        .unique()
        .cloned()
        .collect::<Vec<Address>>();
    let indexer_logs = indexers
        .iter()
        .map(|i| {
            let logs = log_lines
                .iter()
                .filter(|l| &l.indexer == i)
                .cloned()
                .collect();
            (i.clone(), logs)
        })
        .collect::<HashMap<Address, Vec<LogLine>>>();
    let characteristics = indexer_logs
        .into_iter()
        .map(|(address, group)| {
            let first = group.first().unwrap();
            let reliability = group
                .iter()
                .map(|l| if l.success { 1.0 } else { 0.0 })
                .sum::<f64>()
                / group.len() as f64;
            let allocation = &first.allocation;
            let allocation = allocations.iter().find(|a| &a.id == allocation).unwrap();
            let price = group
                .iter()
                .map(|l| l.fee.clone())
                .reduce(|a, f| a + f)
                .unwrap()
                / group.len().try_into().unwrap();
            let blocks_behind =
                group.iter().map(|l| l.blocks_behind).sum::<u64>() / group.len() as u64;
            IndexerCharacteristics {
                address,
                url: first.url.clone(),
                price,
                blocks_behind,
                reliability,
                latency_ms: group.iter().map(|l| l.response_time_ms).collect(),
                stake: allocation.indexer.staked_tokens.shift(),
                allocation: allocation.allocated_tokens.shift(),
            }
        })
        .collect::<Vec<IndexerCharacteristics>>();

    eprintln!("initializing ISA...");

    let utility_config = UtilityConfig::default();
    // set budget above maximum price
    let budget = log_lines.iter().map(|l| l.fee).max().unwrap() + "0.00001".parse().unwrap();
    // generate enough blocks for max blocks_behind
    let blocks = log_lines.iter().map(|l| l.blocks_behind).max().unwrap();
    let blocks = gen_blocks(&(0u64..blocks).into_iter().collect::<Vec<u64>>());
    let latest = blocks.last().unwrap();

    let mut isa = State::default();
    apply_state_update(
        &mut isa,
        &Update::SlashingPercentage("0.1".parse().unwrap()),
    );
    apply_state_update(
        &mut isa,
        &Update::USDToGRTConversion("1.0".parse().unwrap()),
    );
    let indexer_updates = characteristics
        .iter()
        .map(|indexer| {
            let info = Arc::new(IndexerInfo {
                url: indexer.url.clone(),
                stake: indexer.stake,
            });
            let indexings = HashMap::from_iter([(
                deployment.clone(),
                IndexingStatus {
                    allocations: Arc::new(HashMap::from([(
                        test_allocation_id(&indexer.address, &deployment),
                        indexer.allocation,
                    )])),
                    cost_model: Some(Ptr::new(default_cost_model(indexer.price))),
                    block: Some(BlockStatus {
                        reported_number: latest.number.saturating_sub(indexer.blocks_behind),
                        blocks_behind: indexer.blocks_behind,
                        behind_reported_block: false,
                    }),
                },
            )]);
            (indexer.address, IndexerUpdate { info, indexings })
        })
        .collect::<HashMap<Address, IndexerUpdate>>();
    apply_state_update(&mut isa, &Update::Indexers(indexer_updates));

    eprintln!("running simulation...");

    // TODO: It would be more accurate to update ISA state at each log entry.

    let mut rng = SmallRng::from_entropy();
    let mut last_decay = log_lines.first().unwrap().timestamp;
    for line in &log_lines {
        let decays = (line.timestamp - last_decay).num_minutes();
        if decays > 0 {
            for _ in 0..decays {
                isa.decay();
            }
            last_decay = line.timestamp;
        }

        let mut context = Context::new("{ a }", "").unwrap();
        let freshness_requirements = FreshnessRequirements {
            minimum_block: None,
            has_latest: true,
        };
        let latest_block = blocks.last().unwrap().number;
        let (selections, _) = isa
            .select_indexers(
                &utility_config,
                &deployment,
                &mut context,
                latest_block,
                &indexers,
                budget,
                &freshness_requirements,
                1,
            )
            .unwrap();
        let selection = match selections.into_iter().next() {
            Some(selection) => selection,
            None => continue,
        };
        let indexer = characteristics
            .iter()
            .find(|i| &i.address == &selection.indexing.indexer)
            .unwrap();
        let duration = Duration::from_millis(*indexer.latency_ms.choose(&mut rng).unwrap());
        let success = rng.gen_bool(indexer.reliability);
        if success {
            isa.observe_successful_query(&selection.indexing, duration);
        } else {
            isa.observe_failed_query(&selection.indexing, duration, false);
        }
        println!(
            "{},{},{},{},{},{},{},{},{}",
            "simulation",
            line.timestamp,
            selection.indexing.indexer,
            selection.score.url,
            success,
            selection.score.fee,
            selection.score.blocks_behind,
            duration.as_millis(),
            selection.score.utility,
        );
    }

    Ok(())
}

struct IndexerCharacteristics {
    address: Address,
    url: URL,
    price: GRT,
    blocks_behind: u64,
    reliability: f64,
    latency_ms: Vec<u64>,
    stake: GRT,
    allocation: GRT,
}

#[derive(Clone)]
struct LogLine {
    timestamp: DateTime<FixedOffset>,
    deployment: SubgraphDeploymentID,
    indexer: Address,
    url: URL,
    allocation: String,
    success: bool,
    fee: GRT,
    blocks_behind: u64,
    response_time_ms: u64,
    utility: f64,
}

fn parse_log_line(line: &str) -> Option<LogLine> {
    if !line.contains("timestamp") {
        return None;
    }
    let json_start = line.find(|c| c == '{')?;
    let raw = serde_json::from_str::<RawLogLine>(&line.split_at(json_start).1).ok()?;
    if &raw.fields.message != "Indexer attempt" {
        return None;
    }
    Some(LogLine {
        timestamp: DateTime::parse_from_rfc3339(&raw.timestamp).ok()?,
        deployment: raw.fields.deployment,
        indexer: raw.fields.indexer,
        url: raw.fields.url.parse().unwrap(),
        allocation: raw.fields.allocation,
        success: (raw.fields.status_code >> 28) == 0,
        fee: raw.fields.fee.parse::<GRT>().ok()?,
        utility: raw.fields.utility,
        blocks_behind: raw.fields.blocks_behind,
        response_time_ms: raw.fields.response_time_ms,
    })
}

#[derive(Deserialize)]
struct RawLogLine {
    timestamp: String,
    fields: RawLogLineFields,
}

#[derive(Deserialize)]
struct RawLogLineFields {
    message: String,
    deployment: SubgraphDeploymentID,
    indexer: Address,
    allocation: String,
    url: String,
    fee: String,
    utility: f64,
    blocks_behind: u64,
    response_time_ms: u64,
    status_code: u32,
}

#[derive(Deserialize)]
struct AllocationResponse {
    allocations: Vec<Allocation>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Allocation {
    id: String,
    allocated_tokens: GRTWei,
    indexer: Indexer,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Indexer {
    staked_tokens: GRTWei,
}
