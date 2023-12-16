use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::env;
use std::ops::RangeInclusive;

use alloy_primitives::Address;
use anyhow::{bail, ensure};
use prelude::buffer_queue::QueueWriter;
use prelude::test_utils::bytes_from_id;
use prelude::{buffer_queue, double_buffer, UDecimal18, GRT};
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{thread_rng, Rng, RngCore as _, SeedableRng as _};
use thegraph::types::{BlockPointer, DeploymentId};
use tokio::spawn;

use crate::actor::{process_updates, Update};
use crate::{
    BlockRequirements, BlockStatus, Candidate, IndexerError, IndexerErrors, Indexing,
    IndexingState, IndexingStatus, InputError, NetworkParameters, Selection, State,
    UtilityParameters,
};

#[derive(Clone)]
struct Config {
    blocks: RangeInclusive<usize>,
    indexers: RangeInclusive<usize>,
    deployments: RangeInclusive<usize>,
    indexings: RangeInclusive<usize>,
}

#[derive(Debug)]
struct Topology {
    grt_per_usd: GRT,
    slashing_percentage: UDecimal18,
    blocks: Vec<BlockPointer>,
    deployments: HashSet<DeploymentId>,
    indexings: HashMap<Indexing, IndexingStatus>,
    fees: HashMap<Indexing, GRT>,
}

#[derive(Debug)]
struct Request {
    deployment: DeploymentId,
    indexers: Vec<Address>,
    params: UtilityParameters,
}

fn base_indexing_status() -> IndexingStatus {
    IndexingStatus {
        url: "http://localhost:8000".parse().unwrap(),
        stake: GRT(UDecimal18::from(1)),
        allocation: GRT(UDecimal18::from(1)),
        block: Some(BlockStatus {
            reported_number: 0,
            behind_reported_block: false,
            min_block: None,
        }),
    }
}

fn utiliy_params(
    budget: GRT,
    requirements: BlockRequirements,
    latest_block: u64,
) -> UtilityParameters {
    UtilityParameters {
        budget,
        requirements,
        latest_block,
        block_rate_hz: 60.0_f64.recip(),
    }
}

impl Topology {
    async fn gen(
        rng: &mut SmallRng,
        config: &Config,
        update_writer: &mut QueueWriter<Update>,
    ) -> Self {
        let deployments = (0..rng.gen_range(config.deployments.clone()))
            .map(|id| bytes_from_id(id).into())
            .collect();
        let blocks = (0..rng.gen_range(config.blocks.clone()))
            .map(|id| BlockPointer {
                number: id as u64,
                hash: bytes_from_id(id).into(),
            })
            .collect::<Vec<BlockPointer>>();
        let indexings: HashMap<Indexing, IndexingStatus> = (0..rng
            .gen_range(config.indexings.clone()))
            .filter_map(|_| Self::gen_indexing(rng, config, &blocks, &deployments))
            .collect();
        let fees: HashMap<Indexing, GRT> = indexings
            .keys()
            .map(|i| (*i, Self::gen_grt(rng, &[0.0, 0.1, 1.0, 2.0])))
            .collect();
        let state = Self {
            grt_per_usd: GRT(UDecimal18::from(1)),
            slashing_percentage: UDecimal18::try_from(0.1).unwrap(),
            blocks,
            deployments,
            indexings,
            fees,
        };

        update_writer
            .write(Update::GRTPerUSD(state.grt_per_usd))
            .unwrap();
        update_writer
            .write(Update::SlashingPercentage(state.slashing_percentage))
            .unwrap();
        update_writer
            .write(Update::Indexings(state.indexings.clone()))
            .unwrap();

        update_writer.flush().await.unwrap();
        state
    }

    fn gen_indexing(
        rng: &mut SmallRng,
        config: &Config,
        blocks: &[BlockPointer],
        deployments: &HashSet<DeploymentId>,
    ) -> Option<(Indexing, IndexingStatus)> {
        let indexing = Indexing {
            indexer: bytes_from_id(rng.gen_range(config.indexers.clone())).into(),
            deployment: *deployments.iter().choose(rng)?,
        };
        let status = IndexingStatus {
            stake: Self::gen_grt(rng, &[0.0, 50e3, 100e3, 150e3]),
            block: blocks.choose(rng).map(|b| BlockStatus {
                reported_number: b.number,
                behind_reported_block: false,
                min_block: None,
            }),
            ..base_indexing_status()
        };
        Some((indexing, status))
    }

    fn gen_grt(rng: &mut SmallRng, table: &[f64; 4]) -> GRT {
        GRT(UDecimal18::try_from(*table.choose(rng).unwrap()).unwrap())
    }

    fn gen_request(&self, rng: &mut SmallRng) -> Option<Request> {
        let deployment = *self.deployments.iter().choose(rng)?;
        let required_block = match (rng.gen_bool(0.1), self.blocks.choose(rng)) {
            (true, Some(block)) => Some(block.number),
            _ => None,
        };
        Some(Request {
            deployment,
            indexers: self
                .indexings
                .keys()
                .filter(|indexing| indexing.deployment == deployment)
                .map(|indexing| indexing.indexer)
                .collect(),
            params: utiliy_params(
                GRT(UDecimal18::from(1)),
                BlockRequirements {
                    range: required_block.map(|b| (0, b)),
                    has_latest: required_block.is_some() && rng.gen_bool(0.5),
                },
                self.blocks.last()?.number,
            ),
        })
    }

    fn check(
        &self,
        request: &Request,
        result: &Result<(Vec<Selection>, IndexerErrors<'_>), InputError>,
    ) -> anyhow::Result<()> {
        let (selections, errors) = match result {
            Ok((selections, errors)) => (selections, errors),
            Err(_) => bail!("unexpected InputError"),
        };

        let fees = GRT(selections.iter().map(|s| s.fee.0).sum());
        ensure!(fees <= request.params.budget);

        let indexers_dedup: BTreeSet<Address> = request.indexers.iter().copied().collect();
        ensure!(indexers_dedup.len() == request.indexers.len());

        let mut expected_errors = IndexerErrors(BTreeMap::new());
        for indexer in &request.indexers {
            let indexing = Indexing {
                indexer: *indexer,
                deployment: request.deployment,
            };
            let status = self.indexings.get(&indexing).unwrap();
            let required_block = request.params.requirements.range.map(|(_, n)| n);
            let fee = *self.fees.get(&indexing).unwrap();
            println!("indexer={}, fee={:?}", indexer, fee);
            let mut set_err = |err: IndexerError| {
                expected_errors.0.entry(err).or_default().insert(indexer);
            };

            let allowed_block = status.block.as_ref().unwrap().reported_number;
            if matches!(required_block, Some(n) if n.saturating_sub(1) > allowed_block) {
                set_err(IndexerError::MissingRequiredBlock);
            } else if status.block.is_none() {
                set_err(IndexerError::NoStatus);
            } else if status.stake == GRT(UDecimal18::from(0)) {
                set_err(IndexerError::NoStake);
            } else if fee > request.params.budget {
                set_err(IndexerError::FeeTooHigh);
            }
        }
        println!("{:#?}", expected_errors);

        ensure!(errors.0 == expected_errors.0);
        // An indexer must be selected if one exists without an associated error.
        if selections.is_empty() {
            ensure!(errors.0.iter().map(|(_, s)| s.len()).sum::<usize>() == request.indexers.len());
        }

        Ok(())
    }
}

#[tokio::test]
async fn fuzz() {
    // crate::init_tracing(false);

    let seed = env::vars()
        .find(|(k, _)| k == "TEST_SEED")
        .and_then(|(_, v)| v.parse::<u64>().ok())
        .unwrap_or(thread_rng().next_u64());
    println!("TEST_SEED={}", seed);
    let mut rng = SmallRng::seed_from_u64(seed);
    let config = Config {
        blocks: 0..=5,
        indexers: 0..=3,
        deployments: 0..=3,
        indexings: 0..=5,
    };

    for _ in 0..100 {
        let (isa_state, isa_writer) = double_buffer!(State::default());
        let (mut update_writer, update_reader) = buffer_queue::pair();
        spawn(async move {
            process_updates(isa_writer, update_reader).await;
            panic!("ISA actor stopped");
        });

        let topology = Topology::gen(&mut rng, &config, &mut update_writer).await;
        println!("{:#?}", topology);
        let request = match topology.gen_request(&mut rng) {
            Some(request) => request,
            None => continue,
        };
        println!("{:#?}", request);
        let candidates: Vec<Candidate> = request
            .indexers
            .iter()
            .map(|indexer| {
                let indexing = Indexing {
                    indexer: *indexer,
                    deployment: request.deployment,
                };
                Candidate {
                    fee: *topology.fees.get(&indexing).unwrap(),
                    indexing,
                    versions_behind: 0,
                }
            })
            .collect();
        let result = isa_state
            .latest()
            .select_indexers(&mut rng, &request.params, &candidates);
        if let Err(err) = topology.check(&request, &result) {
            println!("{}", err);
            println!("TEST_SEED={}", seed);
            panic!("check failed!");
        }
    }
}

/// All else being equal, select candidates indexing lower subgraph versions.
#[test]
fn favor_higher_version() {
    let mut rng = SmallRng::from_entropy();

    let mut versions_behind = [rng.gen_range(0..3), rng.gen_range(0..3)];
    if versions_behind[0] == versions_behind[1] {
        versions_behind[1] += 1;
    }
    let candidates: Vec<Candidate> = (0..2)
        .map(|i| Candidate {
            indexing: Indexing {
                indexer: bytes_from_id(0).into(),
                deployment: bytes_from_id(i).into(),
            },
            fee: GRT(UDecimal18::from(1)),
            versions_behind: versions_behind[i],
        })
        .collect();

    let mut state = State {
        network_params: NetworkParameters {
            grt_per_usd: Some(GRT(UDecimal18::from(1))),
            slashing_percentage: Some(UDecimal18::try_from(0.1).unwrap()),
        },
        indexings: HashMap::from_iter([]),
    };
    state.indexings.insert(
        candidates[0].indexing,
        IndexingState::new(base_indexing_status()),
    );
    state.indexings.insert(
        candidates[1].indexing,
        IndexingState::new(base_indexing_status()),
    );

    let params = utiliy_params(
        GRT(UDecimal18::from(1)),
        BlockRequirements {
            range: None,
            has_latest: true,
        },
        1,
    );
    let result = state.select_indexers(&mut rng, &params, &candidates);

    println!("{:#?}", candidates);
    println!("{:#?}", versions_behind);
    println!("{:#?}", result);

    let max_version = versions_behind.iter().min().unwrap();
    let index = versions_behind
        .iter()
        .position(|v| v == max_version)
        .unwrap();
    let expected = candidates[index].indexing;

    let selection = result.unwrap().0[0].indexing;
    assert_eq!(selection, expected);
}
