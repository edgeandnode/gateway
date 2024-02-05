use std::time::{Duration, Instant};

use alloy_primitives::U256;
use arrayvec::ArrayVec;
use ordered_float::NotNan;
use rand::{prelude::SliceRandom as _, Rng};
use thegraph::types::UDecimal18;
use toolshed::url::Url;

use crate::performance::performance_utility;
use crate::tokens::GRT;
use crate::utility::{weighted_product_model, UtilityFactor};
use crate::{
    BlockRequirements, ConcaveUtilityParameters, Indexing, Selection, UtilityParameters,
    MIN_SCORE_CUTOFF,
};

#[derive(Debug)]
pub struct SelectionFactors {
    pub indexing: Indexing,
    pub url: Url,
    pub versions_behind: u8,
    pub reliability: f64,
    pub perf_success: f64,
    pub perf_failure: f64,
    pub blocks_behind: u64,
    pub slashable_usd: f64,
    pub expected_score: NotNan<f64>,
    pub fee: GRT,
    pub last_use: Instant,
    pub sybil: NotNan<f64>,
}

pub const SELECTION_LIMIT: usize = 3;

/// A subset of available indexers, with combined utility
struct MetaIndexer<'s>(pub ArrayVec<&'s SelectionFactors, SELECTION_LIMIT>);

type V<T> = ArrayVec<T, SELECTION_LIMIT>;

pub trait ExpectedValue {
    fn expected_value(&self) -> f64;
}

pub fn select_indexers<R: Rng>(
    rng: &mut R,
    params: &UtilityParameters,
    factors: &[SelectionFactors],
) -> Vec<Selection> {
    if factors.is_empty() {
        return vec![];
    }
    // Sample indexer subsets, discarding likely duplicates, retaining the highest scoring subset
    // of available indexers.
    //
    // We must use a suitable indexer, if one exists. Indexers are filtered out when calculating
    // selection factors if they are over budget or don't meet freshness requirements. So they won't
    // pollute the set we're selecting from.
    let sample_limit = factors.len().min(16);
    let mut selections: ArrayVec<&SelectionFactors, SELECTION_LIMIT> = ArrayVec::new();

    // Find the best individual indexer out of some samples to start with.
    for _ in 0..sample_limit {
        let indexer = factors.choose_weighted(rng, |f| *f.sybil).unwrap();
        if selections.is_empty() {
            selections.push(indexer);
        } else if indexer.expected_score > selections[0].expected_score {
            selections[0] = indexer;
        }
    }
    let mut combined_score = selections[0].expected_score;
    // Sample some indexers and add them to the selected set if they increase the combined score.
    for _ in 0..sample_limit {
        if selections.len() == SELECTION_LIMIT {
            break;
        }
        let candidate = factors.choose_weighted(rng, |f| *f.sybil).unwrap();
        let mut meta_indexer = MetaIndexer(selections.clone());
        meta_indexer.0.push(candidate);

        // skip to next iteration if we've already checked this indexer selection.
        if selections
            .iter()
            .any(|s| s.indexing.indexer == candidate.indexing.indexer)
        {
            continue;
        }

        let score = meta_indexer
            .score(params)
            .try_into()
            .expect("NaN multi-selection score");
        if score > combined_score {
            combined_score = score;
            selections.push(candidate);
        }
    }

    selections
        .into_iter()
        .map(|f| Selection {
            indexing: f.indexing,
            url: f.url.clone(),
            fee: f.fee.0.as_u128().unwrap_or(0),
            blocks_behind: f.blocks_behind,
        })
        .collect()
}

impl MetaIndexer<'_> {
    fn fee(&self) -> GRT {
        GRT(self.0.iter().map(|f| f.fee.0).sum())
    }

    fn score(&self, params: &UtilityParameters) -> f64 {
        if self.0.is_empty() {
            return 0.0;
        }

        // Expected values calculated based on the following BQN:
        // # indexer success latencies
        // l ← ⟨50, 20, 100⟩
        // # indexer reliabilities
        // r ← ⟨0.99, 0.5, 0.8⟩
        // # sort both vectors by success latency
        // Sort ← (⍋l)⊏⊢ ⋄ r ↩ Sort r ⋄ l ↩ Sort l
        // pf ← ×`1-r  # ⟨ 0.5 0.005 0.001 ⟩
        // ps ← r×1»pf # ⟨ 0.5 0.495 0.004 ⟩
        // ExpectedValue ← +´ps×⊢
        // # Calculate the expected value for latency under inversion. Since performance utility
        // # has an inverse relationship with utility. We want the values to be pulled toward
        // # infility as reliability decreases.
        // ExpectedValue⌾÷ l # 28.62

        let mut reliability: V<f64> = self.0.iter().map(|f| f.reliability).collect();
        let mut perf_success: V<f64> = self.0.iter().map(|f| f.perf_success).collect();
        let mut slashable_usd: V<f64> = self.0.iter().map(|f| f.slashable_usd).collect();

        let mut permutation =
            permutation::sort_unstable_by_key(&perf_success, |n| NotNan::try_from(*n).unwrap());
        permutation.apply_slice_in_place(&mut reliability);
        permutation.apply_slice_in_place(&mut perf_success);
        permutation.apply_slice_in_place(&mut slashable_usd);

        // BQN: pf ← ×`1-r
        let pf = reliability
            .iter()
            .map(|r| 1.0 - r)
            .scan(1.0, |s, x| {
                *s *= x;
                Some(*s)
            })
            .collect::<V<f64>>();
        // BQN: ps ← r×1»pf
        let ps = std::iter::once(&1.0)
            .chain(&pf)
            .take(SELECTION_LIMIT)
            .zip(&reliability)
            .map(|(p, r)| p * r)
            .collect::<V<f64>>();
        // BQN: (+´ps×⊢)s
        let slashable_usd = slashable_usd.iter().zip(&ps).map(|(a, &b)| a * b).sum();
        // BQN: (+´ps×⊢)⌾÷l
        let perf_success = perf_success
            .iter()
            .zip(&ps)
            .map(|(a, &b)| a.recip() * b)
            .sum::<f64>()
            .recip();
        // BQN: ⌈´f
        let perf_failure = *self
            .0
            .iter()
            .map(|f| NotNan::try_from(f.perf_failure).unwrap())
            .max()
            .unwrap();

        // We use the max value of blocks behind to account for the possibility of incorrect
        // indexing statuses.
        let blocks_behind = self.0.iter().map(|f| f.blocks_behind).max().unwrap();
        let versions_behind = self.0.iter().map(|f| f.versions_behind).max().unwrap();
        let min_last_use = self.0.iter().map(|f| f.last_use).max().unwrap();

        let exploration = exploration_weight(Instant::now().duration_since(min_last_use));
        let p_success = ps.iter().sum::<f64>();
        debug_assert!((0.0..=1.0).contains(&p_success));

        let factors = [
            reliability_utility(p_success).mul_weight(exploration),
            performance_utility(perf_success as u32).mul_weight(exploration * p_success),
            performance_utility(perf_failure as u32).mul_weight(exploration * (1.0 - p_success)),
            economic_security_utility(slashable_usd),
            versions_behind_utility(versions_behind),
            data_freshness_utility(params.block_rate_hz, &params.requirements, blocks_behind),
            fee_utility(&self.fee(), &params.budget),
        ];
        let score = weighted_product_model(factors);

        tracing::trace!(
            indexers = ?self.0.iter().map(|f| f.indexing.indexer).collect::<V<_>>(),
            score,
            ?factors,
        );

        score
    }
}

#[allow(clippy::too_many_arguments)]
pub fn expected_individual_score(
    params: &UtilityParameters,
    reliability: f64,
    perf_success: f64,
    versions_behind: u8,
    blocks_behind: u64,
    slashable_usd: f64,
    zero_allocation: bool,
    fee: &GRT,
) -> f64 {
    let altruism_penalty = UtilityFactor::one(if zero_allocation { 0.8 } else { 1.0 });
    weighted_product_model([
        reliability_utility(reliability),
        performance_utility(perf_success as u32),
        economic_security_utility(slashable_usd),
        versions_behind_utility(versions_behind),
        data_freshness_utility(params.block_rate_hz, &params.requirements, blocks_behind),
        fee_utility(fee, &params.budget),
        altruism_penalty,
    ])
}

// https://www.desmos.com/calculator/dxgonxuihk
fn economic_security_utility(slashable_usd: f64) -> UtilityFactor {
    ConcaveUtilityParameters {
        a: 4e-4,
        weight: 1.5,
    }
    .concave_utility(slashable_usd)
}

/// https://www.desmos.com/calculator/plpijnbvhu
fn reliability_utility(p_success: f64) -> UtilityFactor {
    UtilityFactor::one(p_success.powi(7))
}

/// https://www.desmos.com/calculator/mioowuofsj
fn data_freshness_utility(
    block_rate_hz: f64,
    requirements: &BlockRequirements,
    blocks_behind: u64,
) -> UtilityFactor {
    let weight = 2.0;
    // Add utility if the latest block is requested. Otherwise, data freshness is not a utility,
    // but a binary of minimum block. Note that it can be both.
    if !requirements.has_latest || (blocks_behind == 0) {
        UtilityFactor {
            utility: 1.0,
            weight,
        }
    } else {
        ConcaveUtilityParameters {
            a: 32.0 * block_rate_hz,
            weight,
        }
        .concave_utility(1.0 / blocks_behind as f64)
    }
}

fn versions_behind_utility(versions_behind: u8) -> UtilityFactor {
    UtilityFactor {
        utility: MIN_SCORE_CUTOFF.powi(versions_behind as i32),
        weight: 1.0,
    }
}

/// Decrease utility factor weight of indexers as their time since last use increases.
/// Results in approximately 50% weight at t=60 and 10% weight at t=200.
/// https://www.desmos.com/calculator/j2s3d4tem8
fn exploration_weight(t: Duration) -> f64 {
    0.1_f64.powf(0.005 * t.as_secs_f64())
}

/// Target an optimal fee of ~(1/3) of budget, since up to 3 indexers can be selected.
/// https://www.desmos.com/calculator/elzlqpb7tc
pub fn fee_utility(fee: &GRT, budget: &GRT) -> UtilityFactor {
    // Any fee over budget has zero utility.
    if fee > budget {
        return UtilityFactor::one(0.0);
    }
    let one_wei = UDecimal18::from_raw_u256(U256::from(1));
    let scaled_fee = fee.0 / budget.0.saturating_add(one_wei);
    // (5_f64.sqrt() - 1.0) / 2.0
    const S: f64 = 0.6180339887498949;
    let mut utility = (f64::from(scaled_fee) + S).recip() - S;
    // Set minimum utility, since small negative utility can result from loss of precision when the
    // fee approaches the budget.
    utility = utility.max(1e-18);
    let weight: f64 = 1.4;
    UtilityFactor { utility, weight }
}
