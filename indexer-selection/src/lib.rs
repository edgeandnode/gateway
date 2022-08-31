pub mod actor;
mod block_requirements;
pub mod decay;
mod economic_security;
mod performance;
mod price_efficiency;
mod reputation;
mod selection_factors;
pub mod test_utils;
#[cfg(test)]
mod tests;
mod utility;

pub use crate::{
    block_requirements::freshness_requirements,
    block_requirements::BlockRequirements,
    selection_factors::{BlockStatus, IndexingStatus, SelectionFactors},
};
use crate::{block_requirements::make_query_deterministic, economic_security::*};
use async_trait::async_trait;
pub use cost_model::{self, CostModel};
use num_traits::identities::Zero as _;
pub use ordered_float::NotNan;
use prelude::{epoch_cache::EpochCache, weighted_sample::WeightedSample, *};
use rand::{thread_rng, Rng as _};
pub use receipts;
pub use secp256k1::SecretKey;
use std::{collections::HashMap, sync::Arc};
use url::Url;
use utility::*;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

pub struct Selection {
    indexing: Indexing,
    score: IndexerScore,
    scoring_sample: ScoringSample,
}

pub struct ScoringSample(pub Option<(Address, Result<IndexerScore, SelectionError>)>);

#[derive(Clone, Debug)]
pub struct IndexerQuery {
    pub network: String,
    pub indexing: Indexing,
    pub query: String,
    pub score: IndexerScore,
    pub block_number: u64,
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
    BehindMinimumBlock,
    MissingIndexingStatus,
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
    MissingAllocation,
    UnattestableError,
    Timeout,
    UnexpectedPayload,
    UnresolvedBlock,
    Other(String),
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

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

#[async_trait]
pub trait BlockResolver {
    fn latest_block(&self) -> Option<BlockPointer>;
    fn skip_latest(&mut self, skip: usize);
    async fn resolve_block(
        &self,
        unresolved: UnresolvedBlock,
    ) -> Result<BlockPointer, UnresolvedBlock>;
}

#[derive(Clone, Debug)]
pub struct IndexerScore {
    pub url: Url,
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

#[derive(Default)]
pub struct State {
    pub network_params: NetworkParameters,
    indexers: EpochCache<Address, Arc<IndexerInfo>, 2>,
    indexings: EpochCache<Indexing, SelectionFactors, 2>,
    pub special_indexers: Option<Arc<HashMap<Address, NotNan<f64>>>>,
}

#[derive(Debug)]
pub struct IndexerInfo {
    pub url: Url,
    pub stake: GRT,
}

impl State {
    pub fn insert_indexing(&mut self, indexing: Indexing, status: IndexingStatus) {
        let selection_factors = self
            .indexings
            .get_or_insert(indexing, |_| SelectionFactors::default());
        selection_factors.set_status(status);
    }

    pub fn observe_successful_query(&mut self, indexing: &Indexing, duration: Duration) {
        if let Some(selection_factors) = self.indexings.get_mut(indexing) {
            selection_factors.observe_successful_query(duration);
        }
    }

    pub fn observe_failed_query(&mut self, indexing: &Indexing, duration: Duration, timeout: bool) {
        if let Some(selection_factors) = self.indexings.get_mut(indexing) {
            selection_factors.observe_failed_query(duration, timeout);
        };
    }

    pub fn observe_indexing_behind(
        &mut self,
        indexing: &Indexing,
        block_queried: u64,
        latest_block: u64,
    ) {
        if let Some(selection_factors) = self.indexings.get_mut(indexing) {
            selection_factors.observe_indexing_behind(block_queried, latest_block);
        }
    }

    pub fn penalize(&mut self, indexing: &Indexing, weight: u8) {
        if let Some(selection_factors) = self.indexings.get_mut(indexing) {
            selection_factors.penalize(weight);
        }
    }

    pub fn decay(&mut self) {
        self.indexings.apply(|sf| sf.decay());
    }

    pub async fn select_indexer(
        &self,
        config: &UtilityConfig,
        network: &str,
        subgraph: &SubgraphDeploymentID,
        indexers: &[Address],
        context: &mut Context<'_>,
        block_resolver: &impl BlockResolver,
        freshness_requirements: &BlockRequirements,
        budget: GRT,
    ) -> Result<Option<(IndexerQuery, ScoringSample)>, SelectionError> {
        let selection = match self.make_selection(
            config,
            subgraph,
            context,
            block_resolver,
            &indexers,
            budget,
            &freshness_requirements,
        ) {
            Ok(Some(result)) => result,
            Ok(None) => return Ok(None),
            Err(err) => return Err(err),
        };

        let head = block_resolver
            .latest_block()
            .ok_or(SelectionError::MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let query_block = block_resolver
            .resolve_block(UnresolvedBlock::WithNumber(
                head.number.saturating_sub(selection.score.blocks_behind),
            ))
            .await?;
        let query = make_query_deterministic(
            context,
            block_resolver,
            &query_block,
            selection.score.blocks_behind,
        )
        .await?;

        let indexer_query = IndexerQuery {
            network: network.into(),
            indexing: selection.indexing,
            query: query.query,
            score: selection.score,
            block_number: query_block.number,
        };
        Ok(Some((indexer_query, selection.scoring_sample)))
    }

    /// Select random indexer, weighted by utility. Indexers with incomplete data or that do not
    /// meet the minimum requirements will be excluded.
    fn make_selection(
        &self,
        config: &UtilityConfig,
        deployment: &SubgraphDeploymentID,
        context: &mut Context<'_>,
        block_resolver: &impl BlockResolver,
        indexers: &[Address],
        budget: GRT,
        freshness_requirements: &BlockRequirements,
    ) -> Result<Option<Selection>, SelectionError> {
        let mut scores = Vec::new();
        let mut high_fee_count = 0;
        let mut scoring_sample = WeightedSample::new();
        for indexer in indexers {
            let indexing = Indexing {
                indexer: *indexer,
                deployment: *deployment,
            };
            let result = self.score_indexer(
                &indexing,
                context,
                block_resolver,
                budget,
                config,
                freshness_requirements,
            );
            // TODO: these logs are currently required for data science. However, we would like to omit these in production and only use the sampled scoring logs.
            match &result {
                Ok(score) => tracing::info!(
                    ?indexing.deployment,
                    ?indexing.indexer,
                    ?score.fee,
                    ?score.slashable,
                    %score.utility,
                    %score.sybil,
                    ?score.blocks_behind,
                ),
                Err(err) => tracing::info!(
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

        let sample = scoring_sample
            .take()
            .filter(|(address, _)| address != &indexing.indexer);
        Ok(Some(Selection {
            indexing: indexing.clone(),
            score,
            scoring_sample: ScoringSample(sample),
        }))
    }

    fn score_indexer(
        &self,
        indexing: &Indexing,
        context: &mut Context<'_>,
        block_resolver: &impl BlockResolver,
        budget: GRT,
        config: &UtilityConfig,
        freshness_requirements: &BlockRequirements,
    ) -> Result<IndexerScore, SelectionError> {
        let mut aggregator = UtilityAggregator::new();
        let indexer = self
            .indexers
            .get_unobserved(&indexing.indexer)
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        let economic_security = self
            .network_params
            .economic_security_utility(indexer.stake, config.economic_security)
            .ok_or(SelectionError::MissingNetworkParams)?;
        aggregator.add(economic_security.utility);

        let selection_factors = self
            .indexings
            .get_unobserved(&indexing)
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;

        let latest_block = block_resolver
            .latest_block()
            .ok_or(SelectionError::MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let blocks_behind = selection_factors.blocks_behind()?;

        let (fee, price_efficiency) =
            selection_factors.get_price(context, config.price_efficiency, &budget)?;
        aggregator.add(price_efficiency);

        let indexer_allocation = selection_factors.total_allocation();
        if indexer_allocation == GRT::zero() {
            return Err(SelectionError::NoAllocation(indexing.clone()));
        }

        let performance = selection_factors.expected_performance_utility(config.performance);
        aggregator.add(performance);

        let reputation = selection_factors.expected_reputation_utility(UtilityParameters {
            a: 3.0,
            weight: 1.0,
        });
        aggregator.add(reputation);

        let data_freshness = selection_factors.expected_freshness_utility(
            freshness_requirements,
            config.data_freshness,
            latest_block.number,
        )?;
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
            .as_ref()
            .and_then(|map| map.get(&indexing.indexer).map(|w| **w))
            .unwrap_or(1.0);

        Ok(IndexerScore {
            url: indexer.url.clone(),
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

        // There is a GIP out there which would allow for allocations with 0 GRT stake.
        // For example, MIPS. We don't want for those to never be selected. Furthermore,
        // we can account for the cost of an allocation which would contribute to sybil.
        const BONUS: f64 = 1000.0;

        // Don't flatten so quickly, since numbers are large
        const SLOPE: f64 = 100.0;

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
        let sybil = (((identity + BONUS) / SLOPE) + 1.0).log(std::f64::consts::E);
        NotNan::new(sybil).map_err(|_| BadIndexerReason::NaN)
    }
}

// https://www.desmos.com/calculator/cwgj4ne5ow
const UTILITY_CONFIGS_ECONOMIC_SECURITY: (UtilityParameters, UtilityParameters) = (
    UtilityParameters {
        a: 0.0008,
        weight: 1.0,
    },
    UtilityParameters {
        a: 0.0004,
        weight: 1.5,
    },
);
// https://www.desmos.com/calculator/w6pxajuuve
const UTILITY_CONFIGS_PERFORMANCE: (UtilityParameters, UtilityParameters) = (
    UtilityParameters {
        a: 1.1,
        weight: 1.0,
    },
    UtilityParameters {
        a: 1.2,
        weight: 1.5,
    },
);
// https://www.desmos.com/calculator/uvt9txau4n
const UTILITY_CONFIGS_DATA_FRESHNESS: (UtilityParameters, UtilityParameters) = (
    UtilityParameters {
        a: 5.0,
        weight: 3.0,
    },
    UtilityParameters {
        a: 3.0,
        weight: 4.0,
    },
);
// Don't over or under value "getting a good deal"
// Note: This is only a weight.
const UTILITY_CONFIGS_PRICE_EFFICIENCY: (f64, f64) = (0.5, 1.0);

impl UtilityConfig {
    pub fn from_preferences(preferences: &IndexerPreferences) -> Self {
        fn interp(a: f64, b: f64, x: f64) -> f64 {
            a + ((b - a) * x)
        }
        fn interp_utility(
            bounds: (UtilityParameters, UtilityParameters),
            x: f64,
        ) -> UtilityParameters {
            UtilityParameters {
                a: interp(bounds.0.a, bounds.1.a, x),
                weight: interp(bounds.0.weight, bounds.1.weight, x),
            }
        }
        Self {
            economic_security: interp_utility(
                UTILITY_CONFIGS_ECONOMIC_SECURITY,
                preferences.economic_security,
            ),
            performance: interp_utility(UTILITY_CONFIGS_PERFORMANCE, preferences.performance),
            data_freshness: interp_utility(
                UTILITY_CONFIGS_DATA_FRESHNESS,
                preferences.data_freshness,
            ),
            price_efficiency: interp(
                UTILITY_CONFIGS_PRICE_EFFICIENCY.0,
                UTILITY_CONFIGS_PRICE_EFFICIENCY.1,
                preferences.price_efficiency,
            ),
        }
    }
}

impl Default for UtilityConfig {
    fn default() -> Self {
        Self {
            economic_security: UTILITY_CONFIGS_ECONOMIC_SECURITY.0,
            performance: UTILITY_CONFIGS_PERFORMANCE.0,
            data_freshness: UTILITY_CONFIGS_DATA_FRESHNESS.0,
            price_efficiency: UTILITY_CONFIGS_PRICE_EFFICIENCY.0,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexerPreferences {
    pub economic_security: f64,
    pub performance: f64,
    pub data_freshness: f64,
    pub price_efficiency: f64,
}
