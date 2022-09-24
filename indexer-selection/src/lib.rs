pub mod actor;
pub mod decay;
mod economic_security;
mod indexing;
mod performance;
mod price_efficiency;
mod reputation;
pub mod test_utils;
#[cfg(test)]
mod tests;
mod utility;

pub use crate::indexing::{BlockStatus, IndexingStatus};
use crate::{economic_security::*, indexing::IndexingState};
pub use cost_model::{self, CostModel};
use num_traits::identities::Zero as _;
pub use ordered_float::NotNan;
use prelude::{epoch_cache::EpochCache, weighted_sample::WeightedSample, *};
use price_efficiency::price_efficiency;
use rand::{prelude::SmallRng, thread_rng, Rng as _, SeedableRng as _};
pub use receipts;
use receipts::BorrowFail;
pub use secp256k1::SecretKey;
use std::{collections::HashMap, sync::Arc};
use utility::*;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

#[derive(Clone, Debug)]
pub struct Selection {
    pub indexing: Indexing,
    pub url: URL,
    pub fee: GRT,
    pub blocks_behind: u64,
    pub slashable: USD,
    pub sybil: NotNan<f64>,
    pub utility: NotNan<f64>,
    pub scores: UtilityScores,
}

#[derive(Clone, Debug)]
pub struct UtilityScores {
    pub economic_security: SelectionFactor,
    pub price_efficiency: SelectionFactor,
    pub data_freshness: SelectionFactor,
    pub performance: SelectionFactor,
    pub reputation: SelectionFactor,
}

pub struct ScoringSample(pub Address, pub Result<Selection, SelectionError>);

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectionError {
    MalformedQuery,
    MissingNetworkParams,
    BadIndexer(BadIndexerReason),
    NoAllocation(Indexing),
    BehindMinimumBlock(usize),
    FeesTooHigh(usize),
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

impl From<BorrowFail> for IndexerError {
    fn from(from: BorrowFail) -> Self {
        match from {
            BorrowFail::NoAllocation => Self::MissingAllocation,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
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

#[derive(Default, Debug, Eq, PartialEq)]
pub struct FreshnessRequirements {
    /// If specified, the subgraph must have indexed up to at least this number.
    pub minimum_block: Option<u64>,
    /// If true, the query has an unspecified block which means the query benefits from syncing as
    /// far in the future as possible.
    pub has_latest: bool,
}

#[derive(Default)]
pub struct State {
    pub network_params: NetworkParameters,
    indexers: EpochCache<Address, Arc<IndexerInfo>, 2>,
    indexings: EpochCache<Indexing, IndexingState, 2>,
    pub special_indexers: Option<Arc<HashMap<Address, NotNan<f64>>>>,
}

#[derive(Debug)]
pub struct IndexerInfo {
    pub url: URL,
    pub stake: GRT,
}

impl State {
    pub fn insert_indexing(&mut self, indexing: Indexing, status: IndexingStatus) {
        let selection_factors = self
            .indexings
            .get_or_insert(indexing, |_| IndexingState::default());
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
        latest_query_block: u64,
        latest_block: u64,
    ) {
        if let Some(selection_factors) = self.indexings.get_mut(indexing) {
            selection_factors.observe_indexing_behind(latest_query_block, latest_block);
        }
    }

    pub fn penalize(&mut self, indexing: &Indexing, weight: u8) {
        if let Some(selection_factors) = self.indexings.get_mut(indexing) {
            selection_factors.penalize(weight);
        }
    }

    pub fn decay(&mut self) {
        self.indexings.apply(|s| s.decay());
    }

    /// Select random indexers, weighted by utility. Indexers with incomplete data or that do not
    /// meet the minimum requirements will be excluded. Indexers will continue to be selected until
    /// the `selection_limit` is reached, the budget is exhausted, or no remaining indexers can
    /// increase the overall utility of the selected set.
    pub fn select_indexers(
        &self,
        deployment: &SubgraphDeploymentID,
        indexers: &[Address],
        config: &UtilityConfig,
        freshness_requirements: &FreshnessRequirements,
        context: &mut Context<'_>,
        latest_block: u64,
        budget: GRT,
        selection_limit: u8,
    ) -> Result<(Vec<Selection>, Option<ScoringSample>), SelectionError> {
        let mut scores = Vec::new();
        let mut sampler = WeightedSample::new();
        let mut behind_min_block_count = 0;
        let mut fee_too_high_count = 0;
        for indexer in indexers {
            let indexing = Indexing {
                indexer: *indexer,
                deployment: *deployment,
            };
            let result = self.score_indexer(
                config,
                freshness_requirements,
                context,
                latest_block,
                &budget,
                selection_limit,
                indexing,
            );
            sampler.add((indexer, result.clone()), 1.0);
            match result {
                Ok(score) => scores.push(score),
                Err(SelectionError::MissingNetworkParams) => {
                    return Err(SelectionError::MissingNetworkParams)
                }
                Err(SelectionError::BadIndexer(reason)) => match reason {
                    BadIndexerReason::FeeTooHigh => fee_too_high_count += 1,
                    BadIndexerReason::BehindMinimumBlock => behind_min_block_count += 1,
                    _ => (),
                },
                _ => (),
            };
        }
        if scores.is_empty() {
            match (behind_min_block_count, fee_too_high_count) {
                (0, 0) => (),
                (b, f) if b > f => return Err(SelectionError::BehindMinimumBlock(b)),
                (_, f) => return Err(SelectionError::FeesTooHigh(f)),
            }
        }

        let mut selections = Vec::new();
        let mut rng = SmallRng::from_rng(thread_rng()).unwrap();
        let mut cost = GRT::zero();
        for _ in 0..selection_limit {
            let max_utility = match scores.iter().map(|score| score.utility).max() {
                Some(n) => n,
                _ => break,
            };
            // Having a random utility cutoff that is weighted toward 1 normalized to the highest
            // score makes it so that we define our selection based on an expected utility
            // distribution, so that even if there are many bad indexers with lots of stake it may
            // not adversely affect the result. This is important because an Indexer deployed on the
            // other side of the world should not generally bring our expected utility down below
            // the minimum requirements set forth by this equation.
            let mut utility_cutoff = NotNan::<f64>::new(rng.gen()).unwrap();
            // Careful raising this value, it's really powerful. Near 0 and utility is ignored
            // (only stake matters). Near 1 utility matters at ~ x^2 (depending on the distribution
            // of stake). Above that and things are getting crazy and we're exploiting the utility
            // strongly.
            const UTILITY_PREFERENCE: f64 = 1.1;
            utility_cutoff = max_utility * (1.0 - utility_cutoff.powf(UTILITY_PREFERENCE));
            let mut sampler = WeightedSample::new();
            for (i, score) in scores
                .iter()
                .filter(|score| score.utility >= utility_cutoff)
                .enumerate()
            {
                sampler.add(i, *score.sybil);
            }
            match sampler.take() {
                Some(i) if scores[i].utility > NotNan::zero() => {
                    selections.push(scores.swap_remove(i))
                }
                _ => break,
            };

            cost += selections.last().unwrap().fee;
            let thresholds = Self::utility_thresholds(&selections);
            let adds_utility = |s: &Selection| -> bool {
                (s.scores.economic_security.utility > thresholds.economic_security)
                    || (s.scores.data_freshness.utility > thresholds.data_freshness)
                    || (s.scores.performance.utility > thresholds.performance)
                    || (s.scores.reputation.utility > thresholds.reputation)
            };
            scores
                .retain(|selection| ((cost + selection.fee) <= budget) && adds_utility(selection));
        }

        let sample = sampler
            .take()
            .filter(|(address, _)| selections.iter().all(|s| &s.indexing.indexer != *address))
            .map(|(address, result)| ScoringSample(*address, result));

        Ok((selections, sample))
    }

    fn score_indexer(
        &self,
        config: &UtilityConfig,
        freshness_requirements: &FreshnessRequirements,
        context: &mut Context<'_>,
        latest_block: u64,
        budget: &GRT,
        selection_limit: u8,
        indexing: Indexing,
    ) -> Result<Selection, SelectionError> {
        let info = self
            .indexers
            .get_unobserved(&indexing.indexer)
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        let state = self
            .indexings
            .get_unobserved(&indexing)
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        let status = state
            .status
            .block
            .as_ref()
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        let fee = state.fee(context, config.price_efficiency, &budget, selection_limit)?;
        let EconomicSecurity {
            utility: economic_security,
            slashable,
        } = self
            .network_params
            .economic_security_utility(info.stake, config.economic_security)
            .ok_or(SelectionError::MissingNetworkParams)?;
        let price_efficiency = price_efficiency(&fee, config.price_efficiency, &budget);
        let performance = state.performance.expected_utility(config.performance);
        let reputation = state.reputation.expected_utility(UtilityParameters {
            a: 3.0,
            weight: 1.0,
        });
        let data_freshness = Self::expected_freshness_utility(
            &status,
            freshness_requirements,
            config.data_freshness,
            latest_block,
        )?;
        let utility = weighted_product_model([
            economic_security,
            price_efficiency,
            performance,
            reputation,
            data_freshness,
        ]);
        Ok(Selection {
            indexing,
            url: info.url.clone(),
            fee,
            slashable,
            utility: NotNan::new(utility).map_err(|_| BadIndexerReason::NaN)?,
            scores: UtilityScores {
                economic_security,
                price_efficiency,
                performance,
                reputation,
                data_freshness,
            },
            sybil: Self::sybil(&state.total_allocation())?,
            blocks_behind: status.blocks_behind,
        })
    }

    fn expected_freshness_utility(
        status: &BlockStatus,
        requirements: &FreshnessRequirements,
        utility_parameters: UtilityParameters,
        latest_block: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        // Check that the indexer has synced at least up to any minimum block required.
        if let Some(minimum) = requirements.minimum_block {
            let indexer_latest = latest_block.saturating_sub(status.blocks_behind);
            if indexer_latest < minimum {
                return Err(BadIndexerReason::BehindMinimumBlock.into());
            }
        }
        // Add utility if the latest block is requested. Otherwise, data freshness is not a utility,
        // but a binary of minimum block. Note that it can be both.
        let utility = if !requirements.has_latest || (status.blocks_behind == 0) {
            1.0
        } else {
            let freshness = 1.0 / status.blocks_behind as f64;
            concave_utility(freshness, utility_parameters.a)
        };
        Ok(SelectionFactor {
            utility,
            weight: utility_parameters.weight,
        })
    }

    /// Sybil protection
    fn sybil(indexer_allocation: &GRT) -> Result<NotNan<f64>, BadIndexerReason> {
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

    fn utility_thresholds(selections: &[Selection]) -> UtilityThresholds {
        // Set the thresholds for retaining indexers to the next round based on their ability to add
        // utility over the selected set.
        macro_rules! utility_threshold {
            ($n:ident) => {
                selections
                    .iter()
                    .map(|s| NotNan::new(s.scores.$n.utility).unwrap())
                    .max()
                    .unwrap()
            };
        }
        UtilityThresholds {
            economic_security: *utility_threshold!(economic_security),
            data_freshness: *utility_threshold!(data_freshness),
            performance: *utility_threshold!(performance),
            reputation: *utility_threshold!(reputation),
        }
    }
}

#[derive(Default)]
struct UtilityThresholds {
    economic_security: f64,
    data_freshness: f64,
    performance: f64,
    reputation: f64,
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
