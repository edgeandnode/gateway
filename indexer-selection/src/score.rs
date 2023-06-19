use crate::{
    fee::fee_utility,
    performance::performance_utility,
    utility::{weighted_product_model, UtilityFactor},
    BlockRequirements, ConcaveUtilityParameters, Indexing, Selection, UtilityParameters,
    MIN_SCORE_CUTOFF,
};
use arrayvec::ArrayVec;
use ordered_float::NotNan;
use prelude::{
    rand::{prelude::SliceRandom as _, Rng},
    *,
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
    selection_limit: u8,
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
    let selection_limit = SELECTION_LIMIT.min(selection_limit as usize);
    let mut selections = (ArrayVec::new(), 0.0);
    let mut masks = ArrayVec::<[u8; 20], 20>::new();
    // Calculate a sample limit using a "good enough" approximation of the binomial coefficient of
    // `(factors.len(), SELECTION_LIMIT)` (AKA "n choose k").
    let sample_limit = match factors.len() {
        n if n <= selection_limit => 1,
        n if n == (selection_limit + 1) => n,
        n => (n - selection_limit) * 2,
    }
    .min(masks.capacity());
    for _ in 0..sample_limit {
        let mut meta_indexer = MetaIndexer(
            factors
                .choose_multiple_weighted(rng, selection_limit, |f| f.sybil)
                .unwrap()
                .collect(),
        );
        while (meta_indexer.fee() > params.budget) && !meta_indexer.0.is_empty() {
            // The order of indexers from `choose_multiple_weighted` is unspecified and may not be
            // shuffled. So we should remove a random entry.
            let index = rng.gen_range(0..meta_indexer.0.len());
            meta_indexer.0.remove(index);
        }
        // Don't bother scoring if we've already tried the same subset of indexers.
        let mask = meta_indexer.mask();
        if masks.iter().any(|m| m == &mask) {
            continue;
        }
        masks.push(mask);
        let score = meta_indexer.score(params);
        if score > selections.1 {
            selections = (meta_indexer.0, score);
        }
    }
    selections
        .0
        .into_iter()
        .map(|f| Selection {
            indexing: f.indexing,
            url: f.url.clone(),
            fee: f.fee,
            blocks_behind: f.blocks_behind,
        })
        .collect()
}

impl MetaIndexer<'_> {
    fn fee(&self) -> GRT {
        self.0.iter().map(|f| f.fee).sum()
    }

    fn mask(&self) -> [u8; 20] {
        let mut mask = [0u8; 20];
        for indexer in self.0.iter().map(|f| &f.indexing.indexer) {
            for i in 0..mask.len() {
                mask[i] |= indexer[i];
            }
        }
        mask
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
            performance_utility(params.performance, perf_success as u32)
                .mul_weight(exploration * p_success),
            performance_utility(params.performance, perf_failure as u32)
                .mul_weight(exploration * (1.0 - p_success)),
            params.economic_security.concave_utility(slashable_usd),
            versions_behind_utility(versions_behind),
            data_freshness_utility(params.data_freshness, &params.requirements, blocks_behind),
            fee_utility(params.fee_weight, &self.fee(), &params.budget),
        ];
        let score = weighted_product_model(factors);

        tracing::warn!(
            indexers = ?self.0.iter().map(|f| f.indexing.indexer).collect::<V<_>>(),
            score,
            ?factors,
        );

        score
    }
}

pub fn expected_individual_score(
    params: &UtilityParameters,
    reliability: f64,
    perf_success: f64,
    blocks_behind: u64,
    slashable_usd: f64,
    fee: &GRT,
) -> f64 {
    weighted_product_model([
        reliability_utility(reliability),
        performance_utility(params.performance, perf_success as u32),
        params.economic_security.concave_utility(slashable_usd),
        data_freshness_utility(params.data_freshness, &params.requirements, blocks_behind),
        fee_utility(params.fee_weight, fee, &params.budget),
    ])
}

/// https://www.desmos.com/calculator/plpijnbvhu
fn reliability_utility(p_success: f64) -> UtilityFactor {
    UtilityFactor::one(p_success.powi(7))
}

/// https://www.desmos.com/calculator/6unqha22hp
/// 9f6c6cb0-0e49-4bc4-848e-22a1599af45b
fn data_freshness_utility(
    params: ConcaveUtilityParameters,
    requirements: &BlockRequirements,
    blocks_behind: u64,
) -> UtilityFactor {
    // Add utility if the latest block is requested. Otherwise, data freshness is not a utility,
    // but a binary of minimum block. Note that it can be both.
    if !requirements.has_latest || (blocks_behind == 0) {
        UtilityFactor {
            utility: 1.0,
            weight: params.weight,
        }
    } else {
        params.concave_utility(1.0 / blocks_behind as f64)
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
