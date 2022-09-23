pub mod actor;
pub mod decay;
mod economic_security;
mod indexing;
mod performance;
mod price_efficiency;
mod reputation;
mod score;
pub mod test_utils;
#[cfg(test)]
mod tests;
mod utility;

pub use crate::{
    indexing::{BlockStatus, IndexingStatus},
    score::IndexerScore,
};
pub use cost_model::{self, CostModel};
pub use ordered_float::NotNan;
pub use receipts;
pub use secp256k1::SecretKey;

use crate::{economic_security::*, indexing::IndexingState, score::*};
use num_traits::identities::Zero as _;
use prelude::{epoch_cache::EpochCache, weighted_sample::WeightedSample, *};
use rand::{thread_rng, Rng as _};
use receipts::BorrowFail;
use std::{collections::HashMap, sync::Arc};
use utility::*;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

#[derive(Debug)]
pub struct Selection {
    pub indexing: Indexing,
    pub score: IndexerScore,
}

pub struct ScoringSample(pub Address, pub Result<IndexerScore, SelectionError>);

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectionError {
    MalformedQuery,
    MissingNetworkParams,
    BadIndexer(BadIndexerReason),
    NoAllocation(Indexing),
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
        config: &UtilityConfig,
        deployment: &SubgraphDeploymentID,
        context: &mut Context<'_>,
        latest_block: u64,
        indexers: &[Address],
        budget: GRT,
        freshness_requirements: &FreshnessRequirements,
        selection_limit: u8,
    ) -> Result<(Vec<Selection>, Option<ScoringSample>), SelectionError> {
        let indexing = |indexer: Address| -> Indexing {
            Indexing {
                indexer,
                deployment: *deployment,
            }
        };
        let mut high_fee_count = 0;
        let mut selection_factors = indexers
            .iter()
            .filter_map(|indexer| {
                let indexing = indexing(*indexer);
                let indexer_info = self.indexers.get_unobserved(indexer)?;
                let state = self.indexings.get_unobserved(&indexing)?;
                let fee =
                    match state.fee(context, config.price_efficiency, &budget, selection_limit) {
                        Ok(fee) => fee,
                        Err(_) => {
                            high_fee_count += 1;
                            return None;
                        }
                    };
                let factors = SelectionFactors::new(fee, &indexing, &indexer_info, &state).ok()?;
                Some((indexer, factors))
            })
            .collect::<HashMap<&Address, SelectionFactors>>();

        let mut selections = Vec::<Selection>::new();
        let mut cost = GRT::zero();
        let mut scoring_sample = WeightedSample::new();
        let mut accumulated_utility = NotNan::zero();
        for selection_count in 0..selection_limit {
            let mut scores = Vec::<(&Address, IndexerScore)>::new();
            for (indexer, factors) in &selection_factors {
                let mut score_result = factors.score(
                    config,
                    &self.network_params,
                    freshness_requirements,
                    &budget,
                    &cost,
                    latest_block,
                );
                if let Some(special_indexers) = &self.special_indexers {
                    if let Ok(score) = &mut score_result {
                        if let Some(weight) = special_indexers.get(indexer) {
                            score.utility *= weight;
                        }
                    }
                }
                tracing::trace!(?score_result);
                if let Err(SelectionError::MissingNetworkParams) = &score_result {
                    return Err(SelectionError::MissingNetworkParams);
                }
                if selection_count == 0 {
                    scoring_sample.add((indexer.clone(), score_result.clone()), 1.0);
                }
                let score = match score_result {
                    Ok(score) if score.utility > NotNan::zero() => score,
                    _ => continue,
                };
                scores.push((indexer, score));
            }
            scores.retain(|(indexer, score)| {
                if score.utility < accumulated_utility {
                    selection_factors.remove(indexer);
                    return false;
                }
                true
            });
            if scores.is_empty() {
                break;
            }

            let max_utility = match scores.iter().map(|(_, score)| score.utility).max() {
                Some(n) => n,
                _ => break,
            };
            // Having a random utility cutoff that is weighted toward 1 normalized to the highest
            // score makes it so that we define our selection based on an expected utility
            // distribution, so that even if there are many bad indexers with lots of stake it may
            // not adversely affect the result. This is important because an Indexer deployed on the
            // other side of the world should not generally bring our expected utility down below
            // the minimum requirements set forth by this equation.
            let mut utility_cutoff = NotNan::<f64>::new(thread_rng().gen()).unwrap();
            // Careful raising this value, it's really powerful. Near 0 and utility is ignored
            // (only stake matters). Near 1 utility matters at ~ x^2 (depending on the distribution
            // of stake). Above that and things are getting crazy and we're exploiting the utility
            // strongly.
            const UTILITY_PREFERENCE: f64 = 1.1;
            utility_cutoff = max_utility * (1.0 - utility_cutoff.powf(UTILITY_PREFERENCE));
            let mut selected = WeightedSample::new();
            let scores = scores
                .into_iter()
                .filter(|(_, score)| score.utility >= utility_cutoff);
            for (indexer, score) in scores {
                let sybil = score.sybil.into();
                selected.add((indexer, score), sybil);
            }
            let selection = match selected.take() {
                Some((indexer, score)) => Selection {
                    indexing: indexing(*indexer),
                    score,
                },
                None => break,
            };

            accumulated_utility = selection.score.utility;
            cost += selection.score.fee;
            selection_factors = selection_factors
                .drain()
                .filter(|(indexer, _)| *indexer != &selection.indexing.indexer)
                .filter(|(_, factors)| (factors.fee + cost) <= budget)
                .filter_map(|(indexer, mut factors)| {
                    let indexer_info = self.indexers.get_unobserved(indexer)?;
                    let indexing_state = self.indexings.get_unobserved(&indexing(*indexer))?;
                    factors.merge_selection(indexer_info, indexing_state).ok()?;
                    Some((indexer, factors))
                })
                .collect();

            selections.push(selection);
        }

        if selections.is_empty() && (high_fee_count > 0) {
            return Err(SelectionError::FeesTooHigh(high_fee_count));
        }

        let sample = scoring_sample
            .take()
            .filter(|(address, _)| selections.iter().all(|s| &s.indexing.indexer != *address))
            .map(|(address, result)| ScoringSample(*address, result));

        Ok((selections, sample))
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
