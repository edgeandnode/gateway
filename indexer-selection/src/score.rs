use crate::{
    fee::fee_utility,
    performance::performance_utility,
    utility::{weighted_product_model, UtilityFactor},
    Indexing, Selection, UtilityParameters,
};
use arrayvec::ArrayVec;
use ordered_float::NotNan;
use prelude::{
    rand::{prelude::SliceRandom as _, Rng},
    *,
};

pub struct SelectionFactors {
    pub indexing: Indexing,
    pub url: URL,
    pub reliability: f64,
    pub perf_success: f64,
    pub perf_failure: f64,
    pub blocks_behind: u64,
    pub slashable_usd: f64,
    pub fee: GRT,
    pub last_use: Option<Instant>,
    pub sybil: NotNan<f64>,
}

const SELECTION_LIMIT: usize = 3;

/// A subset of available indexers, with combined utility
struct MetaIndexer<'s>(pub ArrayVec<&'s SelectionFactors, SELECTION_LIMIT>);

type V<T> = ArrayVec<T, SELECTION_LIMIT>;

pub trait ExpectedValue {
    fn expected_value(&self) -> f64;
}

pub fn select_indexers<'s, R: Rng>(
    rng: &mut R,
    params: &UtilityParameters,
    factors: &'s [SelectionFactors],
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
    let mut selections = (ArrayVec::new(), 0.0);
    let mut masks = ArrayVec::<[u8; 20], 20>::new();
    // Calculate a sample limit using a "good enough" approximation of the binomial coefficient of
    // `(factors.len(), SELECTION_LIMIT)` (AKA "n choose k").
    let sample_limit = match factors.len() {
        n if n <= SELECTION_LIMIT => 1,
        n if n == (SELECTION_LIMIT + 1) => n,
        n => (n - SELECTION_LIMIT) * 2,
    }
    .min(masks.capacity());
    for _ in 0..sample_limit {
        let mut meta_indexer = MetaIndexer(
            factors
                .choose_multiple_weighted(rng, SELECTION_LIMIT, |f| f.sybil)
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
        tracing::trace!(
            indexers = ?meta_indexer.0.iter().map(|f| f.indexing.indexer).collect::<V<_>>(),
            score,
        );
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

        // Expected values calculated based on the following BQN (https://tinyurl.com/3vmpdcza):
        // # indexer success latencies
        // l ← ⟨50, 20, 100⟩
        // # indexer reliabilities
        // r ← ⟨0.99, 0.5, 0.8⟩
        // # sort both vectors by success latency
        // Sort ← (⍋l)⊏⊢ ⋄ r ↩ Sort r ⋄ l ↩ Sort l
        // ps ← r×1»×`1-r # ⟨ 0.5 0.495 0.004 ⟩
        // ExpectedValue ← +´ps×⊢
        // ExpectedValue l # 35.15

        let mut reliability: V<f64> = self.0.iter().map(|f| f.reliability).collect();
        let mut perf_success: V<f64> = self.0.iter().map(|f| f.perf_success).collect();
        let mut slashable_usd: V<f64> = self.0.iter().map(|f| f.slashable_usd).collect();

        let mut order = (0..perf_success.len()).collect::<V<usize>>();
        order.sort_unstable_by_key(|i| NotNan::try_from(perf_success[*i]).unwrap());
        macro_rules! sort_by_perf_success {
            ($v:ident) => {
                for i in 0..$v.len() {
                    $v.swap(i, order[i]);
                }
            };
        }
        sort_by_perf_success!(reliability);
        sort_by_perf_success!(perf_success);
        sort_by_perf_success!(slashable_usd);

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
        let expected_value = |v: &V<f64>| -> f64 { v.iter().zip(&ps).map(|(a, &b)| a * b).sum() };
        let perf_success = expected_value(&perf_success);
        let slashable_usd = expected_value(&slashable_usd);

        let perf_failure = *self
            .0
            .iter()
            .map(|f| NotNan::try_from(f.perf_failure).unwrap())
            .max()
            .unwrap();
        // We use the max value of blocks behind to account for the possibility of incorrect
        // indexing statuses.
        let blocks_behind = self.0.iter().map(|f| f.blocks_behind).max().unwrap();
        let min_last_use = self.0.iter().map(|f| f.last_use).max().unwrap();
        weighted_product_model([
            performance_utility(params.performance, perf_success as u32),
            performance_utility(params.performance, perf_failure as u32),
            params.economic_security.concave_utility(slashable_usd),
            params.data_freshness.concave_utility(blocks_behind as f64),
            fee_utility(params.fee_weight, &self.fee(), &params.budget),
            exploration(min_last_use),
        ])
    }
}

/// Increase utility of indexers as their last use increases.
/// https://www.desmos.com/calculator/qcxa0tbigg
fn exploration(last_use: Option<Instant>) -> UtilityFactor {
    let secs_since_use = last_use
        .map(|t| Instant::now().duration_since(t))
        .unwrap_or(Duration::from_secs(60))
        .as_secs_f64();
    UtilityFactor::one((secs_since_use + 6.0).log(6.0))
}
