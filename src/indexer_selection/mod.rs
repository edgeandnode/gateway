mod allocations;
mod block_requirements;
mod data_freshness;
pub mod decay;
mod economic_security;
mod indexers;
mod performance;
mod price_efficiency;
mod reputation;
mod selection_factors;
mod utility;

#[cfg(test)]
pub mod test_utils;
#[cfg(test)]
mod tests;

pub use crate::indexer_selection::{
    allocations::{Allocations, QueryStatus, Receipt},
    block_requirements::BlockRequirements,
    indexers::{IndexerDataReader, IndexerDataWriter},
    price_efficiency::CostModelSource,
    selection_factors::{IndexingData, IndexingStatus, SelectionFactors},
};
use crate::{
    block_resolver::BlockResolver,
    indexer_selection::{block_requirements::*, economic_security::*},
    prelude::{
        shared_lookup::{SharedLookup, SharedLookupWriter},
        weighted_sample::WeightedSample,
        *,
    },
};
use cost_model;
use lazy_static::lazy_static;
use num_traits::identities::Zero as _;
pub use ordered_float::NotNan;
use prometheus;
use rand::{thread_rng, Rng as _};
pub use secp256k1::SecretKey;
use std::{collections::HashMap, sync::Arc};
use utility::*;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

pub struct Selection {
    indexing: Indexing,
    score: IndexerScore,
    receipt: Receipt,
    scoring_sample: ScoringSample,
}

pub struct ScoringSample(pub Option<(Address, Result<IndexerScore, SelectionError>)>);

#[derive(Clone, Debug)]
pub struct IndexerQuery {
    pub network: String,
    pub indexing: Indexing,
    pub query: String,
    pub receipt: Receipt,
    pub score: IndexerScore,
    pub allocation: Address,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectionError {
    BadInput,
    MissingNetworkParams,
    MissingBlock(UnresolvedBlock),
    BadIndexer(BadIndexerReason),
    NoAllocation(Indexing),
    FeesTooHigh(usize),
}

impl From<UnresolvedBlock> for SelectionError {
    fn from(unresolved: UnresolvedBlock) -> Self {
        Self::MissingBlock(unresolved)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BadIndexerReason {
    MissingIndexerStake,
    BehindMinimumBlock,
    MissingIndexingStatus,
    MissingCostModel,
    QueryNotCosted,
    FeeTooHigh,
    NaN,
}

impl From<BadIndexerReason> for SelectionError {
    fn from(err: BadIndexerReason) -> Self {
        Self::BadIndexer(err)
    }
}

#[derive(Clone, Debug)]
pub enum IndexerError {
    NoAttestation,
    Panic,
    Timeout,
    UnexpectedPayload,
    UnresolvedBlock,
    Other(String),
}

impl IndexerError {
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum UnresolvedBlock {
    WithHash(Bytes32),
    WithNumber(u64),
}

impl UnresolvedBlock {
    pub fn matches(&self, block: &BlockPointer) -> bool {
        match self {
            Self::WithHash(hash) => &block.hash == hash,
            Self::WithNumber(number) => &block.number == number,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Indexing {
    pub indexer: Address,
    pub deployment: SubgraphDeploymentID,
}

#[derive(Clone, Debug)]
pub struct UtilityConfig {
    pub economic_security: UtilityParameters,
    pub performance: UtilityParameters,
    pub data_freshness: UtilityParameters,
    pub price_efficiency: f64,
}

#[derive(Clone, Debug)]
pub struct IndexerScore {
    pub url: Arc<String>,
    pub fee: GRT,
    pub slashable: USD,
    pub utility: NotNan<f64>,
    pub utility_scores: UtilityScores,
    pub sybil: NotNan<f64>,
    pub blocks_behind: u64,
}

#[derive(Clone, Debug)]
pub struct UtilityScores {
    pub economic_security: f64,
    pub price_efficiency: f64,
    pub data_freshness: f64,
    pub performance: f64,
    pub reputation: f64,
}

pub struct Inputs {
    pub slashing_percentage: Eventual<PPM>,
    pub usd_to_grt_conversion: Eventual<USD>,
    pub indexers: SharedLookup<Address, IndexerDataReader>,
    pub indexings: SharedLookup<Indexing, SelectionFactors>,
    pub special_indexers: Eventual<HashMap<Address, NotNan<f64>>>,
}

pub struct InputWriters {
    pub slashing_percentage: EventualWriter<PPM>,
    pub usd_to_grt_conversion: EventualWriter<USD>,
    pub indexers: SharedLookupWriter<Address, IndexerDataReader, IndexerDataWriter>,
    pub indexings: SharedLookupWriter<Indexing, SelectionFactors, IndexingData>,
    pub special_indexers: EventualWriter<HashMap<Address, NotNan<f64>>>,
}

pub struct Indexers {
    pub network_params: NetworkParameters,
    indexers: SharedLookup<Address, IndexerDataReader>,
    indexings: SharedLookup<Indexing, SelectionFactors>,
    special_indexers: Eventual<HashMap<Address, NotNan<f64>>>,
}

impl Indexers {
    pub fn inputs() -> (InputWriters, Inputs) {
        let (slashing_percentage_writer, slashing_percentage) = Eventual::new();
        let (usd_to_grt_conversion_writer, usd_to_grt_conversion) = Eventual::new();
        let (indexers_writer, indexers) = SharedLookup::new();
        let (indexings_writer, indexings) = SharedLookup::new();
        let (special_indexers_writer, special_indexers) = Eventual::new();
        (
            InputWriters {
                slashing_percentage: slashing_percentage_writer,
                usd_to_grt_conversion: usd_to_grt_conversion_writer,
                indexers: indexers_writer,
                indexings: indexings_writer,
                special_indexers: special_indexers_writer,
            },
            Inputs {
                slashing_percentage,
                usd_to_grt_conversion,
                indexers,
                indexings,
                special_indexers,
            },
        )
    }

    pub fn new(inputs: Inputs) -> Indexers {
        Indexers {
            network_params: NetworkParameters {
                slashing_percentage: inputs.slashing_percentage,
                usd_to_grt_conversion: inputs.usd_to_grt_conversion,
            },
            indexers: inputs.indexers,
            indexings: inputs.indexings,
            special_indexers: inputs.special_indexers,
        }
    }

    pub async fn observe_successful_query(
        &self,
        indexing: &Indexing,
        duration: Duration,
        receipt: &[u8],
    ) {
        let selection_factors = match self.indexings.get(indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors
            .observe_successful_query(duration, receipt)
            .await;
    }

    pub async fn observe_failed_query(
        &self,
        indexing: &Indexing,
        receipt: &[u8],
        error: &IndexerError,
    ) {
        let selection_factors = match self.indexings.get(indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors.observe_failed_query(receipt, error).await;
    }

    pub async fn observe_indexing_behind(
        &self,
        context: &mut Context<'_>,
        indexing: &Indexing,
        block_resolver: &BlockResolver,
    ) {
        // Get this early to be closer to the time when the query was made so
        // that race conditions occur less frequently. They will still occur,
        // but less is better.
        let latest = block_resolver.latest_block().map(|b| b.number).unwrap_or(0);
        let freshness_requirements =
            freshness_requirements(&mut context.operations, block_resolver).await;
        let selection_factors = match self.indexings.get(indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        let minimum_block = match freshness_requirements {
            Ok(requirements) => requirements.minimum_block,
            // If we observed a block hash in the query that we could no longer associate with a
            // number, then we have detected a reorg and the indexer receives no penalty.
            Err(_) => return,
        };
        selection_factors
            .observe_indexing_behind(minimum_block, latest)
            .await;
    }

    pub async fn penalize(&self, indexing: &Indexing, weight: u8) {
        let selection_factors = match self.indexings.get(indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors.penalize(weight).await;
    }

    pub async fn decay(&self) {
        for indexing in self.indexings.keys().await {
            if let Some(selection_factors) = self.indexings.get(&indexing).await {
                selection_factors.decay().await;
            }
        }
    }

    pub async fn freshness_requirements(
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
    ) -> Result<BlockRequirements, SelectionError> {
        freshness_requirements(&mut context.operations, block_resolver).await
    }

    pub async fn select_indexer(
        &self,
        config: &UtilityConfig,
        network: &str,
        subgraph: &SubgraphDeploymentID,
        indexers: &[Address],
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        freshness_requirements: &BlockRequirements,
        budget: GRT,
    ) -> Result<Option<(IndexerQuery, ScoringSample)>, SelectionError> {
        let selection = match self
            .make_selection(
                config,
                subgraph,
                context,
                block_resolver,
                &indexers,
                budget,
                &freshness_requirements,
            )
            .await
        {
            Ok(Some(result)) => result,
            Ok(None) => return Ok(None),
            Err(err) => return Err(err),
        };

        let make_query_deterministic_timer =
            METRICS.make_query_deterministic_duration.start_timer();
        let head = block_resolver
            .latest_block()
            .ok_or(SelectionError::MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let latest_block = block_resolver
            .resolve_block(UnresolvedBlock::WithNumber(
                head.number.saturating_sub(selection.score.blocks_behind),
            ))
            .await?;
        let query = make_query_deterministic(
            context,
            block_resolver,
            &latest_block,
            selection.score.blocks_behind,
        )
        .await?;
        make_query_deterministic_timer.observe_duration();

        let mut allocation = Address([0; 20]);
        allocation
            .0
            .copy_from_slice(&selection.receipt.commitment[0..20]);

        let indexer_query = IndexerQuery {
            network: network.into(),
            indexing: selection.indexing,
            query: query.query,
            receipt: selection.receipt.commitment.into(),
            score: selection.score,
            allocation,
        };
        Ok(Some((indexer_query, selection.scoring_sample)))
    }

    /// Select random indexer, weighted by utility. Indexers with incomplete data or that do not
    /// meet the minimum requirements will be excluded.
    async fn make_selection(
        &self,
        config: &UtilityConfig,
        deployment: &SubgraphDeploymentID,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        indexers: &[Address],
        budget: GRT,
        freshness_requirements: &BlockRequirements,
    ) -> Result<Option<Selection>, SelectionError> {
        let _make_selection_timer = METRICS.make_selection_duration.start_timer();
        let mut scores = Vec::new();
        let mut high_fee_count = 0;
        let mut scoring_sample = WeightedSample::new();
        for indexer in indexers {
            let indexing = Indexing {
                indexer: *indexer,
                deployment: *deployment,
            };
            let result = self
                .score_indexer(
                    &indexing,
                    context,
                    block_resolver,
                    budget,
                    config,
                    freshness_requirements,
                )
                .await;
            match &result {
                Ok(score) => tracing::trace!(
                    ?indexing.deployment,
                    ?indexing.indexer,
                    ?score.fee,
                    ?score.slashable,
                    %score.utility,
                    %score.sybil,
                    ?score.blocks_behind,
                ),
                Err(err) => tracing::trace!(
                    ?indexing.deployment,
                    ?indexing.indexer,
                    score_err = ?err,
                ),
            };
            scoring_sample.add((indexing.indexer, result.clone()), 1.0);
            match &result {
                Err(err) => match err {
                    &SelectionError::BadInput
                    | &SelectionError::MissingNetworkParams
                    | &SelectionError::MissingBlock(_) => return Err(err.clone()),
                    &SelectionError::BadIndexer(BadIndexerReason::FeeTooHigh) => {
                        high_fee_count += 1;
                    }
                    _ => (),
                },
                _ => (),
            };
            let score = match result {
                Ok(score) if score.utility > NotNan::zero() => score,
                _ => continue,
            };
            scores.push((indexing, score));
        }
        let max_utility = match scores.iter().map(|(_, score)| score.utility).max() {
            Some(n) => n,
            _ if high_fee_count > 0 => return Err(SelectionError::FeesTooHigh(high_fee_count)),
            _ => return Ok(None),
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
        let mut selected = WeightedSample::new();
        let scores = scores
            .into_iter()
            .filter(|(_, score)| score.utility >= utility_cutoff);
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

        let fee = score.fee.clone();
        let sample = scoring_sample
            .take()
            .filter(|(address, _)| address != &indexing.indexer);
        self.indexings
            .get(&indexing)
            .await
            .ok_or(BadIndexerReason::MissingIndexingStatus)?
            .commit(&fee)
            .await
            .map(|receipt| {
                Some(Selection {
                    indexing: indexing.clone(),
                    score,
                    receipt,
                    scoring_sample: ScoringSample(sample),
                })
            })
            .map_err(|_| SelectionError::NoAllocation(indexing.clone()))
    }

    async fn score_indexer(
        &self,
        indexing: &Indexing,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        budget: GRT,
        config: &UtilityConfig,
        freshness_requirements: &BlockRequirements,
    ) -> Result<IndexerScore, SelectionError> {
        let _score_indexers_timer = METRICS.score_indexer_duration.start_timer();
        let mut aggregator = UtilityAggregator::new();
        let indexer_data = self
            .indexers
            .get(&indexing.indexer)
            .await
            .map(|data| (data.url.value_immediate(), data.stake.value_immediate()))
            .unwrap_or_default();
        let indexer_url = indexer_data
            .0
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        let indexer_stake = indexer_data
            .1
            .ok_or(BadIndexerReason::MissingIndexerStake)?;
        let economic_security = self
            .network_params
            .economic_security_utility(indexer_stake, config.economic_security)
            .ok_or(SelectionError::MissingNetworkParams)?;
        aggregator.add(economic_security.utility);

        let selection_factors = self
            .indexings
            .get(&indexing)
            .await
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;

        let latest_block = block_resolver
            .latest_block()
            .ok_or(SelectionError::MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let blocks_behind = selection_factors.blocks_behind().await?;

        let (fee, price_efficiency) = selection_factors
            .get_price(context, config.price_efficiency, &budget)
            .await?;
        aggregator.add(price_efficiency);

        let indexer_allocation = selection_factors.total_allocation().await;
        if indexer_allocation == GRT::zero() {
            return Err(SelectionError::NoAllocation(indexing.clone()));
        }

        let performance = selection_factors
            .expected_performance_utility(config.performance)
            .await;
        aggregator.add(performance);

        let reputation = selection_factors
            .expected_reputation_utility(UtilityParameters {
                a: 3.0,
                weight: 1.0,
            })
            .await;
        aggregator.add(reputation);

        let data_freshness = selection_factors
            .expected_freshness_utility(
                freshness_requirements,
                config.data_freshness,
                latest_block.number,
                blocks_behind,
            )
            .await?;
        aggregator.add(data_freshness);

        drop(selection_factors);

        // It's not immediately obvious why this mult works. We want to consider the amount
        // staked over the total amount staked of all Indexers in the running. But, we don't
        // know the total stake. If we did, it would be dividing all of these by that
        // constant. Dividing all weights by a constant has no effect on the selection
        // algorithm. Interestingly, delegating to an indexer just about guarantees that the
        // indexer will receive more queries if they met the minimum criteria above. So,
        // delegating more and then getting more queries is kind of a self-fulfilling
        // prophesy. What balances this, is that any amount delegated is most productive
        // when delegated proportionally to each Indexer's utility for that subgraph.
        let mut utility = aggregator.crunch();

        // Some indexers require an additional weight applied to their utility. For example,
        // backstop indexers may have a reduced utility.
        utility *= self
            .special_indexers
            .value_immediate()
            .and_then(|map| map.get(&indexing.indexer).map(|w| **w))
            .unwrap_or(1.0);

        Ok(IndexerScore {
            url: indexer_url,
            fee,
            slashable: economic_security.slashable_usd,
            utility: NotNan::new(utility).map_err(|_| BadIndexerReason::NaN)?,
            utility_scores: UtilityScores {
                economic_security: economic_security.utility.utility,
                price_efficiency: price_efficiency.utility,
                data_freshness: data_freshness.utility,
                performance: performance.utility,
                reputation: reputation.utility,
            },
            sybil: Self::sybil(indexer_allocation)?,
            blocks_behind,
        })
    }

    /// Sybil protection
    fn sybil(indexer_allocation: GRT) -> Result<NotNan<f64>, BadIndexerReason> {
        let identity = indexer_allocation.as_f64();
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

const UTILITY_CONFIGS_DEFAULT_INDEX: usize = 1;
const UTILITY_CONFIGS_LEN: usize = 3;
// https://www.desmos.com/calculator/lzlii17feb
const UTILITY_CONFIGS_ECONOMIC_SECURITY: [UtilityParameters; UTILITY_CONFIGS_LEN] = [
    UtilityParameters {
        a: 0.000016,
        weight: 0.5,
    },
    UtilityParameters {
        a: 0.000008,
        weight: 1.0,
    },
    UtilityParameters {
        a: 0.000004,
        weight: 1.5,
    },
];
// https://www.desmos.com/calculator/reykkamaje
const UTILITY_CONFIGS_PERFORMANCE: [UtilityParameters; UTILITY_CONFIGS_LEN] = [
    UtilityParameters {
        a: 0.0080,
        weight: 0.7,
    },
    UtilityParameters {
        a: 0.0032,
        weight: 1.0,
    },
    UtilityParameters {
        a: 0.0016,
        weight: 1.5,
    },
];
// https://www.desmos.com/calculator/hircodefui
const UTILITY_CONFIGS_DATA_FRESHNESS: [UtilityParameters; UTILITY_CONFIGS_LEN] = [
    UtilityParameters {
        a: 6.5,
        weight: 0.5,
    },
    UtilityParameters {
        a: 4.0,
        weight: 1.0,
    },
    UtilityParameters {
        a: 1.8,
        weight: 1.5,
    },
];
// Don't over or under value "getting a good deal"
// Note: This is only a weight.
const UTILITY_CONFIGS_PRICE_EFFICIENCY: [f64; UTILITY_CONFIGS_LEN] = [0.1, 0.5, 1.0];

// TODO: For the user experience we should turn these into 0-1 values,
// and from those calculate both a utility parameter and a weight
// which should be used when combining utilities.
impl UtilityConfig {
    pub fn from_indexes(
        economic_security: i8,
        performance: i8,
        data_freshness: i8,
        price_efficiency: i8,
    ) -> Self {
        let to_index = |i: i8| -> usize {
            let max = UTILITY_CONFIGS_LEN as i8 - 1;
            let default = UTILITY_CONFIGS_DEFAULT_INDEX as i8;
            default.saturating_add(i).max(0).min(max) as usize
        };
        Self {
            economic_security: UTILITY_CONFIGS_ECONOMIC_SECURITY[to_index(economic_security)],
            performance: UTILITY_CONFIGS_PERFORMANCE[to_index(performance)],
            data_freshness: UTILITY_CONFIGS_DATA_FRESHNESS[to_index(data_freshness)],
            price_efficiency: UTILITY_CONFIGS_PRICE_EFFICIENCY[to_index(price_efficiency)],
        }
    }
}

impl Default for UtilityConfig {
    fn default() -> Self {
        let i = UTILITY_CONFIGS_DEFAULT_INDEX as i8;
        Self::from_indexes(i, i, i, i)
    }
}

struct Metrics {
    make_query_deterministic_duration: prometheus::Histogram,
    make_selection_duration: prometheus::Histogram,
    score_indexer_duration: prometheus::Histogram,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics {
        make_query_deterministic_duration: prometheus::register_histogram!(
            "make_query_deterministic_duration",
            "Duration of making query deterministic"
        )
        .unwrap(),
        make_selection_duration: prometheus::register_histogram!(
            "make_selection_duration",
            "Duration of making indexer selection"
        )
        .unwrap(),
        score_indexer_duration: prometheus::register_histogram!(
            "score_indexer_duration",
            "Duration of indexer scoring"
        )
        .unwrap(),
    };
}
