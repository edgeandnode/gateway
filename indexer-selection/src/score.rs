use std::time::SystemTime;

use crate::{
    decay::DecayBuffer,
    performance::Performance,
    price_efficiency::price_efficiency,
    reliability::Reliability,
    utility::{weighted_product_model, UtilityFactor},
    ConcaveUtilityParameters, Indexing, Selection, UtilityParameters,
};
use arrayvec::ArrayVec;
use ordered_float::NotNan;
use prelude::{
    rand::{prelude::SliceRandom as _, Rng},
    *,
};

#[derive(Clone)]
pub struct SelectionFactors<'s> {
    pub indexing: Indexing,
    pub url: URL,
    pub reliability: &'s DecayBuffer<Reliability>,
    pub perf_success: &'s DecayBuffer<Performance>,
    pub perf_failure: &'s DecayBuffer<Performance>,
    pub blocks_behind: u64,
    pub slashable_stake: USD,
    pub price: GRT,
    pub last_use: SystemTime,
    pub sybil: NotNan<f64>,
}

const SELECTION_LIMIT: usize = 3;

/// A subset of available indexers, with combined utility
struct MetaIndexer<'s>(pub ArrayVec<&'s SelectionFactors<'s>, SELECTION_LIMIT>);

impl MetaIndexer<'_> {
    fn selections(self) -> Vec<Selection> {
        self.0
            .into_iter()
            .map(|f| Selection {
                indexing: f.indexing,
                url: f.url.clone(),
                price: f.price,
                blocks_behind: f.blocks_behind,
            })
            .collect()
    }
}

pub trait Sample {
    type Value;
    fn sample(&self, rng: &mut impl Rng) -> Self::Value;
}

pub fn select_indexers<'s>(
    rng: &mut impl Rng,
    params: &UtilityParameters,
    factors: &'s [SelectionFactors<'s>],
) -> Vec<Selection> {
    if factors.is_empty() {
        return vec![];
    }

    let mut meta_indexers = ArrayVec::<MetaIndexer<'s>, 20>::new();
    let mut masks = ArrayVec::<[u8; 20], 20>::new();
    let sample_limit = match factors.len() {
        n if n < (SELECTION_LIMIT * SELECTION_LIMIT) => n,
        n => n * 2,
    }
    .min(meta_indexers.capacity());
    // Sample indexer subsets, discarding likely duplicates.
    for _ in 0..(sample_limit + 2) {
        if meta_indexers.len() == sample_limit {
            break;
        }
        let chosen = factors.choose_multiple_weighted(rng, SELECTION_LIMIT, |f| f.sybil);
        let mut meta_indexer = match chosen {
            Ok(chosen) => MetaIndexer(chosen.collect()),
            Err(err) => unreachable!("{}", err),
        };
        while (meta_indexer.price() > params.budget) && !meta_indexer.0.is_empty() {
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
        .filter_map(|m| NotNan::try_from(m.score(rng, params)).ok())
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
    fn price(&self) -> GRT {
        self.0.iter().map(|f| f.price).sum()
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

    fn score(&self, rng: &mut impl Rng, params: &UtilityParameters) -> f64 {
        let mut success_min_ms = u32::MAX;
        let mut failure_max_ms: u32 = 0;
        let mut slashable_total = USD::zero();
        let mut slashable_count: u32 = 0;
        let mut blocks_behind_max: u64 = 0;
        for _ in 0..16 {
            let mut selected = (u32::MAX, None);
            for factors in &self.0 {
                if !factors.reliability.sample(rng) {
                    failure_max_ms = failure_max_ms.max(factors.perf_failure.sample(rng));
                    continue;
                }
                let success_ms = factors.perf_success.sample(rng);
                if success_ms < selected.0 {
                    selected = (success_ms, Some(factors));
                    success_min_ms = success_min_ms.min(success_ms);
                }
            }
            if let (_, Some(factors)) = selected {
                slashable_total = slashable_total.saturating_add(factors.slashable_stake);
                slashable_count += 1;
                blocks_behind_max = blocks_behind_max.max(factors.blocks_behind);
            }
        }
        let slashable_avg = slashable_total / slashable_count.min(1).try_into().unwrap();
        let cost = self.price();
        let min_last_use = self.0.iter().map(|f| f.last_use).min().unwrap();
        weighted_product_model([
            params.performance.performance_utility(success_min_ms),
            params.performance.performance_utility(failure_max_ms),
            params
                .economic_security
                .concave_utility(slashable_avg.as_f64()),
            params
                .data_freshness
                .concave_utility(blocks_behind_max as f64),
            price_efficiency(&cost, params.price_efficiency_weight, &params.budget),
            confidence(min_last_use),
        ])
    }
}

/// https://www.desmos.com/calculator/pok5u8v2g8
fn confidence(last_use: SystemTime) -> UtilityFactor {
    let params = ConcaveUtilityParameters {
        a: 0.1,
        weight: 0.1,
    };
    let secs_since_use = SystemTime::now()
        .duration_since(last_use)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    params.concave_utility(secs_since_use + 1.0)
}
