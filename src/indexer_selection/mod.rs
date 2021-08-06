mod economic_security;
mod network_cache;
mod performance;
mod price_efficiency;
mod receipts;
mod reputation;
mod utility;

#[cfg(test)]
mod test_utils;

use crate::prelude::{shared_lookup::SharedLookup, weighted_sample::WeightedSample, *};
use cost_model;
use cost_model::CostModel;
use economic_security::*;
use network_cache::*;
pub use ordered_float::NotNan;
use performance::*;
use price_efficiency::*;
use rand::{thread_rng, Rng as _};
use receipts::*;
use reputation::*;
pub use secp256k1::SecretKey;
use std::sync::Arc;
use tokio::{
    sync::{Mutex, RwLock},
    time,
};
use utility::*;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Indexing {
    pub indexer: Address,
    pub subgraph: SubgraphDeploymentID,
}

pub struct IndexerQuery {
    pub indexer: Address,
    pub query: String,
    pub receipt: ReceiptBorrow,
    pub fee: GRT,
    pub slashable_usd: USD,
    pub utility: NotNan<f64>,
    pub blocks_behind: Option<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectionError {
    BadInput,
    MissingNetworkParams,
    MissingBlock(UnresolvedBlock),
    BadIndexer(BadIndexerReason),
    InsufficientCollateral { indexer: Address, fee: GRT },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BadIndexerReason {
    MissingIndexerStake,
    MissingIndexerDelegatedStake,
    BehindMinimumBlock,
    MissingIndexingStatus,
    MissingCostModel,
    QueryNotCosted,
    FeeTooHigh,
    InsufficientCollateral,
    NaN,
}

impl From<BadIndexerReason> for SelectionError {
    fn from(err: BadIndexerReason) -> Self {
        Self::BadIndexer(err)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum UnresolvedBlock {
    WithNumber(u64),
    WithHash(Bytes32),
}

impl From<UnresolvedBlock> for SelectionError {
    fn from(err: UnresolvedBlock) -> Self {
        Self::MissingBlock(err)
    }
}

#[derive(Default, Clone)]
pub struct IndexerUtilityTracker {
    stake: Option<GRT>,
    delegated_stake: Option<GRT>,
}

#[derive(Default)]
struct IndexingData {
    performance: Performance,
    freshness: DataFreshness,
    price_efficiency: PriceEfficiency,
    reputation: Reputation,
    receipts: Receipts,
}

pub struct UtilityConfig {
    economic_security: f64,
    performance: f64,
    data_freshness: f64,
}

pub struct IndexerScore {
    fee: GRT,
    slashable: USD,
    utility: NotNan<f64>,
    sybil: NotNan<f64>,
    blocks_behind: u64,
}

#[derive(Clone)]
pub struct Indexers {
    network_params: Arc<RwLock<NetworkParameters>>,
    network_cache: Arc<RwLock<NetworkCache>>,
    indexings: Arc<SharedLookup<Indexing, IndexingData>>,
    indexers: Arc<SharedLookup<Address, IndexerUtilityTracker>>,
    // last_decay: Arc<Mutex<Option<time::Instant>>>,
}

impl Indexers {
    // TODO: use eventuals to update shared state.

    pub fn new(network_params: Arc<RwLock<NetworkParameters>>) -> Indexers {
        Indexers {
            network_params,
            network_cache: Arc::default(),
            indexings: Arc::default(),
            indexers: Arc::default(),
        }
    }

    pub async fn set_block(&mut self, network: &str, block: BlockPointer) {
        self.network_cache.write().await.set_block(network, block);
    }

    pub async fn set_default_cost_model(&mut self, indexing: &Indexing, price: GRT) {
        self.set_cost_model(indexing, format!("default => {};", price), "{}".into())
            .await;
    }

    pub async fn set_cost_model(&mut self, indexing: &Indexing, model: String, globals: String) {
        let src = model.clone();
        let compiled = CostModel::compile(model, &globals).ok();
        self.indexings
            .with_value_mut(indexing, move |data| {
                data.price_efficiency
                    .set_cost_model(compiled, (src, globals))
            })
            .await;
    }

    pub async fn set_indexing_status(
        &mut self,
        network: &str,
        indexing: &Indexing,
        block_number: u64,
    ) {
        let latest = self
            .network_cache
            .write()
            .await
            .latest_block(network, 0)
            .map(|block| block.number)
            .unwrap_or_default();
        let behind = latest.saturating_sub(block_number);
        self.indexings
            .with_value_mut(indexing, move |data| {
                data.freshness.set_blocks_behind(behind, block_number);
            })
            .await;
    }

    pub async fn set_stake(&mut self, indexer: &Address, stake: GRT, delegated: GRT) {
        self.indexers
            .with_value_mut(indexer, move |data| {
                data.stake = Some(stake);
                data.delegated_stake = Some(delegated);
            })
            .await;
    }

    pub async fn install_receipts_transfer(
        &mut self,
        indexing: &Indexing,
        transfer_id: Bytes32,
        collateral: &GRT,
        secret: SecretKey,
    ) {
        self.indexings
            .with_value_mut(indexing, move |data| {
                data.receipts.add_transfer(transfer_id, collateral, secret);
            })
            .await;
    }

    pub async fn observe_successful_query(
        &mut self,
        indexing: &Indexing,
        duration: time::Duration,
        receipt: &[u8],
    ) {
        self.indexings
            .with_value_mut(indexing, move |data| {
                data.receipts.release(receipt, QueryStatus::Success);
                data.performance.add_successful_query(duration);
                data.reputation.add_successful_query();
            })
            .await;
    }

    pub async fn observe_failed_query(
        &mut self,
        indexing: &Indexing,
        receipt: &[u8],
        is_unknown: bool,
    ) {
        let status = if is_unknown {
            QueryStatus::Unknown
        } else {
            QueryStatus::Failure
        };
        self.indexings
            .with_value_mut(indexing, move |data| {
                data.receipts.release(receipt, status);
                data.reputation.add_failed_query();
            })
            .await;
    }

    // TODO: decay
    // pub async fn decay(&mut self) {
    //     let mut log = match self.last_decay.try_lock() {
    //         Ok(log) => log,
    //         Err(_) => return,
    //     };
    //     let time = time::Instant::now();
    //     let last_decay = match log.replace(time) {
    //         Some(last_decay) => last_decay,
    //         None => return,
    //     };
    //     let passed_hours = (time - last_decay).as_secs_f64() / 3600.0;
    //     // Information half-life of ~24 hours.
    //     let retain = 0.973f64.powf(passed_hours);
    //     // TODO
    // }

    // TODO: Specify budget in terms of a cost model -
    // the budget should be different per query
    pub async fn select_indexer(
        &mut self,
        config: &UtilityConfig,
        network: &str,
        subgraph: &SubgraphDeploymentID,
        indexers: &[Address],
        query: String,
        variables: Option<String>,
        budget: USD,
    ) -> Result<Option<IndexerQuery>, SelectionError> {
        let budget: GRT = self
            .network_params
            .read()
            .await
            .usd_to_grt(budget)
            .ok_or(SelectionError::MissingNetworkParams)?;
        // Performance: Use a shared context to avoid duplicating query parsing,
        // which is one of the most expensive operations.
        let mut context = Context::new(&query, variables.as_deref().unwrap_or_default())
            .map_err(|_| SelectionError::BadInput)?;
        let freshness_requirements = self
            .network_cache
            .read()
            .await
            .freshness_requirements(&mut context.operations, network)?;

        // Select random indexer, weighted by utility. Indexers with incomplete
        // data or that do not meet the minimum requirements will be excluded.
        let mut scores = Vec::new();
        for indexer in indexers {
            let indexing = Indexing {
                indexer: *indexer,
                subgraph: *subgraph,
            };
            let result = self
                .score_indexer(
                    network,
                    &indexing,
                    &mut context,
                    budget,
                    config,
                    &freshness_requirements,
                )
                .await;
            let score = match result {
                Ok(score) => score,
                Err(err) => match &err {
                    &SelectionError::BadInput | &SelectionError::MissingNetworkParams => {
                        return Err(err)
                    }
                    _ => continue,
                },
            };
            scores.push((indexing, score));
        }

        let max_utility = match scores.iter().map(|(_, score)| score.utility).max() {
            Some(n) => n,
            None => return Ok(None),
        };
        // Having a random utility cutoff that is weighted toward 1 normalized
        // to the highest score makes it so that we define our selection based
        // on an expected utility distribution, so that even if there are many
        // bad indexers with lots of stake it may not adversely affect the
        // result. This is important because an Indexer deployed on the other
        // side of the world should not generally bring our expected utility
        // down below the minimum requirements set forth by this equation.
        let mut utility_cutoff = NotNan::<f64>::new(thread_rng().gen()).unwrap();
        // Careful raising this value, it's really powerful. Near 0 and utility
        // is ignored (only stake matters). Near 1 utility matters at ~ x^2
        // (depending on the distribution of stake). Above that and things are
        // getting crazy and we're exploiting the utility strongly.
        const UTILITY_PREFERENCE: f64 = 1.1;
        utility_cutoff = max_utility * (1.0 - utility_cutoff.powf(UTILITY_PREFERENCE));
        let scores = scores
            .into_iter()
            .filter(|(_, score)| score.utility >= utility_cutoff);

        let mut selected = WeightedSample::new();
        for (indexing, score) in scores {
            let sybil = score.sybil.into();
            selected.add((indexing, score), sybil);
        }
        let (indexing, score) = match selected.take() {
            Some(selection) => selection,
            None => return Ok(None),
        };

        // Technically the "algorithm" part ends here, but eventually we want to
        // be able to go back with data already collected if a later step fails.

        // TODO: Depending on how these steps fail, it can make sense to try to
        // kick off resolutions (eg: getting block hashes, or adding collateral)
        // and at the same time try to use another Indexer.

        let query = self.network_cache.write().await.make_query_deterministic(
            network,
            context,
            score.blocks_behind,
        )?;
        let receipt = self
            .indexings
            .with_value_mut(&indexing, |info| info.receipts.commit(&score.fee))
            .await
            .map_err(|_| SelectionError::InsufficientCollateral {
                indexer: indexing.indexer,
                fee: score.fee,
            })?;

        Ok(Some(IndexerQuery {
            indexer: indexing.indexer,
            query: query.query,
            receipt,
            fee: score.fee,
            slashable_usd: score.slashable,
            utility: score.utility,
            blocks_behind: query.blocks_behind,
        }))
    }

    async fn score_indexer(
        &mut self,
        network: &str,
        indexing: &Indexing,
        context: &mut Context<'_>,
        budget: GRT,
        config: &UtilityConfig,
        freshness_requirements: &BlockRequirements,
    ) -> Result<IndexerScore, SelectionError> {
        let mut aggregator = UtilityAggregator::new();
        let (indexer_stake, delegated_stake) = self
            .indexers
            .with_value(
                &indexing.indexer,
                |v| -> Result<(GRT, GRT), BadIndexerReason> {
                    let indexer_stake = v.stake.ok_or(BadIndexerReason::MissingIndexerStake)?;
                    let delegated_stake = v
                        .delegated_stake
                        .ok_or(BadIndexerReason::MissingIndexerDelegatedStake)?;
                    Ok((indexer_stake, delegated_stake))
                },
            )
            .await?;
        let economic_security = self
            .network_params
            .read()
            .await
            .economic_security_utility(indexer_stake, config.economic_security)
            .ok_or(SelectionError::MissingNetworkParams)?;
        aggregator.add(economic_security.utility.clone());

        let blocks_behind = self
            .indexings
            .with_value(&indexing, |data| data.freshness.blocks_behind())
            .await?;
        let latest_block = self
            .network_cache
            .read()
            .await
            .latest_block(network, blocks_behind)?;

        self.indexings
            .with_value(&indexing, |data| {
                aggregator.add(data.performance.expected_utility(config.performance));
                aggregator.add(data.reputation.expected_utility()?);
                aggregator.add(data.freshness.expected_utility(
                    freshness_requirements,
                    config.data_freshness,
                    latest_block.number,
                    blocks_behind,
                )?);
                let (fee, price_efficiency) = data.price_efficiency.get_price(context, &budget)?;
                aggregator.add(price_efficiency);

                if !data.receipts.has_collateral_for(&fee) {
                    return Err(BadIndexerReason::InsufficientCollateral.into());
                }

                // It's not immediately obvious why this mult works. We want to consider the amount
                // staked over the total amount staked of all Indexers in the running. But, we don't
                // know the total stake. If we did, it would be dividing all of these by that
                // constant. Dividing all weights by a constant has no effect on the selection
                // algorithm. Interestingly, delegating to an indexer just about guarantees that the
                // indexer will receive more queries if they met the minimum criteria above. So,
                // delegating more and then getting more queries is kind of a self-fulfilling
                // prophesy. What balances this, is that any amount delegated is most productive
                // when delegated proportionally to each Indexer's utility for that subgraph.
                let utility = aggregator.crunch();

                Ok(IndexerScore {
                    fee,
                    slashable: economic_security.slashable_usd,
                    utility: NotNan::new(utility).map_err(|_| BadIndexerReason::NaN)?,
                    sybil: Self::sybil(indexer_stake, delegated_stake)?,
                    blocks_behind,
                })
            })
            .await
    }

    /// Sybil protection
    /// TODO: This is wrong. It should be looking at the total stake of all
    /// allocations on the Indexing, not the total stake for the Indexer. The
    /// allocations are meant to provide a signal about capacity and should be
    /// respected.
    fn sybil(indexer_stake: GRT, delegated_stake: GRT) -> Result<NotNan<f64>, BadIndexerReason> {
        // This unfortunately double-counts indexer own stake, once for economic
        // security and once for sybil.
        let total_stake = delegated_stake.saturating_add(indexer_stake);
        let identity = total_stake.as_f64();
        // To optimize for sybil protection, we want to just mult the utility by the identity
        // weight. But this may run into some economic problems. Consider the following scenario:
        //
        // Two indexers: A, and B have utilities of 45% and 55%, respectively. They are equally
        // delegated at 50% of the total delegation pool, each. Because their delegation is
        // equal, they receive query fees proportional to their utility. A delegator notices that
        // part of their delegation would be more efficiently allocated if they move it to the
        // indexer with higher utility so that delegation reflects query volume. So, now they have
        // utilities of 45% and 55%, and delegation of 45% and 55%. Because these are multiplied,
        // the new selection is 40% and 60%. But, the delegations are 45% and 55%. So… a delegator
        // notices that their delegation is inefficiently allocated and move their delegation to the
        // more selected indexer. Now, delegation is 40% 60%, but utility stayed at 45% and 55%… and
        // the new selections are 35% and 65%… and so the cycle continues. The gap continues to
        // widen until all delegation is moved to the marginally better Indexer. This is the kind of
        // winner-take-all scenario we are trying to avoid. In the absence of cold hard math and
        // reasoning, going to try using log magics.
        let sybil = (identity.max(0.0) + 1.0).log(std::f64::consts::E);
        NotNan::new(sybil).map_err(|_| BadIndexerReason::NaN)
    }
}

// TODO: For the user experience we should turn these into 0-1 values,
// and from those calculate both a utility parameter and a weight
// which should be used when combining utilities.
impl Default for UtilityConfig {
    fn default() -> Self {
        Self {
            /// This value comes from the PRD and is for the web case, where ~50% of users may leave
            /// a webpage if it takes longer than 2s to load. So - 50% utility at that point. After
            /// playing with this I feel that the emphasis on performance should increase. For one,
            /// the existing utility function is as though this one request were the only factor in
            /// loading a page, when it may have other requests that must go before or after it that
            /// should be included in that 2 second metric. The other is that this doesn't account
            /// for the user experience difference in users that do stay around long enough for the
            /// page to load.
            // Before change to PRD -
            // performance: 1.570744,
            // After change to PRD -
            performance: 0.01,
            /// This value comes from the PRD and makes it such that ~80% of utility is achieved at
            /// $1m, and utility increases linearly near $100k
            // economic_security: 0.00000161757,
            /// But, it turns out that it's hard to get that much slashable stake given the limited
            /// supply of GRT spread out across multiple indexers. So instead, put ~91% utility at
            /// $400k - which matches the testnet of Indexers having ~$5m and slashing at 10% but
            /// not using all of their value to stake
            economic_security: 0.000006,
            /// Strongly prefers latest blocks. Utility is at 1 when 0 blocks behind, .95 at 1 block
            /// behind, and 0.5 at 8 blocks behind.
            data_freshness: 4.33,
            // Don't over or under value "getting a good deal"
            // Note: This is not a utility_a parameter, but is a weight.
            // price_efficiency: 1.0,
            // TODO
            // reputation: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        indexer_selection::test_utils::{gen_blocks, TEST_KEY},
        prelude::test_utils::bytes_from_id,
    };
    use std::collections::{BTreeMap, HashMap};

    struct IndexerCharacteristics {
        stake: GRT,
        delegated_stake: GRT,
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
        let network_params = Arc::new(RwLock::new(NetworkParameters {
            usd_to_grt_conversion: Eventual::from_value(1u64.try_into().unwrap()),
            slashing_percentage: Eventual::from_value("0.1".parse().unwrap()),
        }));
        let mut indexers = Indexers::new(network_params);

        let network = "mainnet";
        let blocks = gen_blocks(&(0u64..100).into_iter().collect::<Vec<u64>>());
        for block in blocks.iter() {
            indexers.set_block(network, block.clone()).await;
        }
        let latest = blocks.last().unwrap();
        let subgraph: SubgraphDeploymentID = bytes_from_id(99).into();
        let tests = [
            // Great!
            IndexerCharacteristics {
                stake: 500000u64.try_into().unwrap(),
                delegated_stake: 600000u64.try_into().unwrap(),
                price: "0.000040".parse().unwrap(),
                latency_ms: 80,
                reliability: 0.999,
                blocks_behind: 0,
            },
            // Also great!
            IndexerCharacteristics {
                stake: 400000u64.try_into().unwrap(),
                delegated_stake: 800000u64.try_into().unwrap(),
                price: "0.000040".parse().unwrap(),
                latency_ms: 70,
                reliability: 0.995,
                blocks_behind: 1,
            },
            // Ok!
            IndexerCharacteristics {
                stake: 300000u64.try_into().unwrap(),
                delegated_stake: 400000u64.try_into().unwrap(),
                price: "0.000034".parse().unwrap(),
                latency_ms: 130,
                reliability: 0.95,
                blocks_behind: 1,
            },
            // Race to the bottom
            IndexerCharacteristics {
                stake: 400000u64.try_into().unwrap(),
                delegated_stake: 40000u64.try_into().unwrap(),
                price: "0.000005".parse().unwrap(),
                latency_ms: 250,
                reliability: 0.96,
                blocks_behind: 1,
            },
            // Meh
            IndexerCharacteristics {
                stake: 100000u64.try_into().unwrap(),
                delegated_stake: 100000u64.try_into().unwrap(),
                price: "0.000024".parse().unwrap(),
                latency_ms: 200,
                reliability: 0.80,
                blocks_behind: 8,
            },
            // Bad
            IndexerCharacteristics {
                stake: 100000u64.try_into().unwrap(),
                delegated_stake: 100000u64.try_into().unwrap(),
                price: "0.000040".parse().unwrap(),
                latency_ms: 1900,
                reliability: 0.80,
                blocks_behind: 8,
            },
            // Overpriced
            IndexerCharacteristics {
                stake: 500000u64.try_into().unwrap(),
                delegated_stake: 600000u64.try_into().unwrap(),
                price: "0.000999".parse().unwrap(),
                latency_ms: 80,
                reliability: 0.999,
                blocks_behind: 0,
            },
            // Optimize economic security
            IndexerCharacteristics {
                stake: 1000000u64.try_into().unwrap(),
                delegated_stake: 400000u64.try_into().unwrap(),
                price: "0.000045".parse().unwrap(),
                latency_ms: 120,
                reliability: 0.99,
                blocks_behind: 1,
            },
            // Optimize performance
            IndexerCharacteristics {
                stake: 400000u64.try_into().unwrap(),
                delegated_stake: 400000u64.try_into().unwrap(),
                price: "0.000040".parse().unwrap(),
                latency_ms: 60,
                reliability: 0.99,
                blocks_behind: 1,
            },
            // Optimize reliability
            IndexerCharacteristics {
                stake: 300000u64.try_into().unwrap(),
                delegated_stake: 400000u64.try_into().unwrap(),
                price: "0.000035".parse().unwrap(),
                latency_ms: 120,
                reliability: 0.999,
                blocks_behind: 0,
            },
        ];

        let mut data = HashMap::new();
        let mut indexer_ids = Vec::new();
        for indexer in tests.iter() {
            let indexing = Indexing {
                indexer: bytes_from_id(indexer_ids.len()).into(),
                subgraph,
            };
            indexers
                .set_default_cost_model(&indexing, indexer.price)
                .await;
            indexers
                .set_indexing_status(network, &indexing, latest.number - indexer.blocks_behind)
                .await;
            indexers
                .set_stake(&indexing.indexer, indexer.stake, indexer.delegated_stake)
                .await;
            data.insert(indexing.indexer, indexer);
            indexer_ids.push(indexing.indexer);
            indexers
                .install_receipts_transfer(
                    &indexing,
                    bytes_from_id(1).into(),
                    &1_000_000_000_000_000u64.try_into().unwrap(),
                    TEST_KEY.parse().unwrap(),
                )
                .await;
        }

        let config = UtilityConfig::default();
        let mut results = BTreeMap::<Address, IndexerResults>::new();

        let query_time = time::Instant::now();
        const COUNT: usize = 86400;
        const QPS: u64 = 2000;
        for i in 0..COUNT {
            let query = "{ a }".to_string();
            let variables = "".to_string();
            let budget: GRT = "0.00005".parse().unwrap();
            let result = indexers
                .select_indexer(
                    &config,
                    network,
                    &subgraph,
                    &indexer_ids,
                    query,
                    Some(variables),
                    budget,
                )
                .await
                .unwrap();

            // TODO
            // This will make almost no difference.
            // Just testing.
            // if i % (COUNT / 10) == 0 {
            //     indexers.decay();
            // }
            let query = match result {
                Some(query) => query,
                None => continue,
            };
            let entry = results.entry(query.indexer).or_default();
            entry.queries_received += 1;
            let data = data.get(&query.indexer).unwrap();
            let indexing = Indexing {
                subgraph,
                indexer: query.indexer,
            };
            if data.reliability > thread_rng().gen() {
                let duration = time::Duration::from_millis(data.latency_ms);
                let receipt = &query.receipt.commitment;
                let fees: GRTWei =
                    primitive_types::U256::from_big_endian(&receipt[(receipt.len() - 32)..])
                        .try_into()
                        .unwrap();
                entry.query_fees = fees.shift();
                indexers
                    .observe_successful_query(&indexing, duration, &query.receipt.commitment)
                    .await;
            } else {
                indexers
                    .observe_failed_query(&indexing, &query.receipt.commitment, false)
                    .await;
            }
        }

        let query_time = time::Instant::now() - query_time;
        println!("Thoughput: {} /s", COUNT as f64 / query_time.as_secs_f64());
        println!("| ID | Stake | Blocks Behind | Price | Latency | Reliability | Daily Fees | Queries Served |");
        println!("| --- | --- | --- | --- | --- | --- | --- | --- |");

        let mut total_fees = GRT::zero();
        for (name, indexer_id) in indexer_ids.iter().enumerate() {
            let data = data.get(indexer_id).unwrap();
            let results = results.get(indexer_id).cloned().unwrap_or_default();

            println!(
                "| {} | {} GRT | {} | {} USD | {} ms | {}% | {} USD | {:.1}% |",
                name,
                data.stake,
                // data.delegated_stake,
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

        // TODO: test snapshot restore
    }
}
