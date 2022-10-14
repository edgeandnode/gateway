use crate::{
    price_efficiency::price_efficiency,
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

impl MetaIndexer<'_> {
    fn selections(self) -> Vec<Selection> {
        self.0
            .into_iter()
            .map(|f| Selection {
                indexing: f.indexing,
                url: f.url.clone(),
                fee: f.fee,
                blocks_behind: f.blocks_behind,
            })
            .collect()
    }
}

pub trait ExpectedValue {
    fn expected_value(&self) -> f64;
}

pub fn select_indexers<'s>(
    rng: &mut impl Rng,
    params: &UtilityParameters,
    factors: &'s [SelectionFactors],
) -> Vec<Selection> {
    if factors.is_empty() {
        return vec![];
    }

    let mut meta_indexers = ArrayVec::<MetaIndexer<'s>, 20>::new();
    let mut masks = ArrayVec::<[u8; 20], 20>::new();
    // Calculate a sample limit using a "good enough" approximation of the binomial coefficient of
    // `(factors.len(), SELECTION_LIMIT)` (AKA "n choose k").
    let sample_limit = match factors.len() {
        n if n <= SELECTION_LIMIT => 1,
        n if n == (SELECTION_LIMIT + 1) => n,
        n => (n - SELECTION_LIMIT) * 2,
    }
    .min(meta_indexers.capacity());
    // Sample indexer subsets, discarding likely duplicates.
    //
    // We must use a suitable indexer, if one exists. Indexers are filtered out when calculating
    // selection factors if they are over budget or don't meet freshness requirements. So they won't
    // pollute the set we're selecting from.
    for _ in 0..sample_limit {
        if meta_indexers.len() == sample_limit {
            break;
        }
        let chosen = factors.choose_multiple_weighted(rng, SELECTION_LIMIT, |f| f.sybil);
        let mut meta_indexer = match chosen {
            Ok(chosen) => MetaIndexer(chosen.collect()),
            Err(err) => unreachable!("{}", err),
        };
        while (meta_indexer.fee() > params.budget) && !meta_indexer.0.is_empty() {
            // The order of indexers from `choose_multiple_weighted` is unspecified and may not be
            // shuffled. So we should remove a random entry.
            let index = rng.gen_range(0..meta_indexer.0.len());
            meta_indexer.0.remove(index);
        }
        if meta_indexer.0.is_empty() {
            continue;
        }
        let mask = meta_indexer.mask();
        if masks.iter().all(|m| m != &mask) {
            meta_indexers.push(meta_indexer);
            masks.push(mask);
        }
    }
    if meta_indexers.is_empty() {
        return vec![];
    }

    let scores = meta_indexers
        .iter()
        .filter_map(|m| NotNan::try_from(m.score(params)).ok())
        .collect::<ArrayVec<NotNan<f64>, 20>>();
    let max_score = scores.iter().copied().max().unwrap();
    meta_indexers
        .into_iter()
        .zip(scores)
        .find(|(_, s)| *s == max_score)
        .unwrap()
        .0
        .selections()
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

        type V<T> = ArrayVec<T, SELECTION_LIMIT>;

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
            .collect::<ArrayVec<f64, SELECTION_LIMIT>>();
        // BQN: ps ← r×1»pf
        let ps = std::iter::once(&1.0)
            .chain(&pf)
            .take(SELECTION_LIMIT)
            .zip(&reliability)
            .map(|(p, r)| p * r)
            .collect::<ArrayVec<f64, SELECTION_LIMIT>>();
        let expected_value = |v: &V<f64>| -> f64 { v.iter().zip(&ps).map(|(a, &b)| a * b).sum() };

        let perf_success = expected_value(&perf_success);
        let slashable_usd = expected_value(&slashable_usd);

        let perf_failure = self
            .0
            .iter()
            .map(|f| NotNan::try_from(f.perf_failure).unwrap())
            .max()
            .unwrap();

        let cost = self.fee();
        // We use the max value of blocks behind to account for the possibility of incorrect
        // indexing statuses.
        let blocks_behind = self.0.iter().map(|f| f.blocks_behind).max().unwrap();
        let min_last_use = self.0.iter().map(|f| f.last_use).max().unwrap();
        weighted_product_model([
            params.performance.performance_utility(perf_success as u32),
            params.performance.performance_utility(*perf_failure as u32),
            params.economic_security.concave_utility(slashable_usd),
            params.data_freshness.concave_utility(blocks_behind as f64),
            price_efficiency(&cost, params.price_efficiency_weight, &params.budget),
            exploration(min_last_use),
        ])
    }
}

/// Increase utility of indexers as their last use increases.
/// https://www.desmos.com/calculator/qcxa0tbigg
fn exploration(last_use: Option<Instant>) -> UtilityFactor {
    let secs_since_use = last_use
        .map(|t| Instant::now().duration_since(t))
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    UtilityFactor::one((secs_since_use + 6.0).log(6.0))
}
