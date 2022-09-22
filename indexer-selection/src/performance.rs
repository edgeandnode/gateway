// Converts latency to utility.
//
// The simplest possible thing would be to convert all queries to utility,
// aggregate utilities and count, then use that to calculate the average utility.
// The problem with that approach is that it doesn't allow us to parameterize
// the utility function.
// Note: avg(utility(latencies)) != utlity(avg(latencies))
//
// So instead we take only a slightly more complicated approach.
// First, we bucket queries keyed by a quantized latency. Each
// bucket is gets a utility based on the consumer preferences,
// and then we get the count-weighted average of utilities.
// This trades off precision for query-time work.

use crate::{
    decay::{Decay, DecayUtility},
    score::Merge,
};
use prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Performance {
    latency: Vec<u32>,
    count: Vec<f64>,
}

impl Merge for Performance {
    fn merge(&mut self, other: &Self) {
        for (count, latency) in other.count.iter().zip(other.latency.iter()) {
            let i = self.latency.partition_point(|x| x < latency);
            match self.latency.get(i) {
                None => {
                    self.latency.push(*latency);
                    self.count.push(*count);
                }
                Some(l) if l == latency => {
                    self.latency[i] += latency;
                    self.count[i] += count;
                }
                Some(_) => {
                    self.latency.insert(i, *latency);
                    self.count.insert(i, *count);
                }
            };
        }
    }
}

impl Decay for Performance {
    fn shift(&mut self, mut next: Option<&mut Self>, fraction: f64, keep: f64) {
        // For each quantized bucket, find the corresponding quantized bucket in
        // the next frame, and shift information into it.
        for (count, latency) in self.count.iter_mut().zip(self.latency.iter().copied()) {
            let next_latency = next.as_deref_mut().map(|n| n.bucket_mut(latency));
            count.shift(next_latency, fraction, keep);
        }
    }

    fn clear(&mut self) {
        self.latency.clear();
        self.count.clear();
    }
}

// https://www.desmos.com/calculator/w6pxajuuve
fn latency_to_utility(latency: f64, pow: f64) -> f64 {
    let sigmoid = |x: f64| 1.0 + std::f64::consts::E.powf((x.powf(pow) - 400.0) / 300.0);
    sigmoid(0.0) / sigmoid(latency as f64)
}

impl DecayUtility for Performance {
    fn expected_utility(&self, pow: f64) -> f64 {
        let mut agg_count = 0.0;
        let mut agg_utility = 0.0;
        for (count, latency) in self.iter() {
            agg_count += count;
            agg_utility += count * latency_to_utility(latency as f64, pow);
        }
        if agg_count == 0.0 {
            return 0.0;
        }
        agg_utility / agg_count
    }
    fn count(&self) -> f64 {
        self.count.iter().sum()
    }
}

impl Performance {
    pub fn add_query(&mut self, mut duration: Duration, status: Result<(), ()>) {
        // If the query is failed, the user will experience it as increased latency.
        // Furthermore, most errors are expected to resolve quicker than successful queries.
        // Penalizing failed queries in performance will make it so that we account
        // for the additional latency incurred by retry. Ideally we could do this by
        // accounting for the likelyhood that the utility curve would be moved down and how
        // much later when selecting, but this is a much simpler LOC.
        if status.is_err() {
            duration *= 2;
        }
        let quantized = nearest_triangle_number(duration.as_secs_f64() * 1000.0) as u32;
        *self.bucket_mut(quantized) += 1.0;
    }

    fn bucket_mut(&mut self, key: u32) -> &mut f64 {
        let index = match self.latency.binary_search(&key) {
            Ok(index) => index,
            Err(index) => {
                self.latency.insert(index, key);
                self.count.insert(index, 0.0);
                index
            }
        };
        &mut self.count[index]
    }

    fn iter<'a>(&'a self) -> impl 'a + Iterator<Item = (f64, u32)> {
        self.count.iter().cloned().zip(self.latency.iter().cloned())
    }
}

fn nearest_triangle_number(n: f64) -> f64 {
    let m = (0.5 * ((1.0 + 8.0 * n).sqrt() - 1.0)).floor();
    let tan0 = (m * m + m) / 2.0;
    let tan1 = (m * m + 3.0 * m + 2.0) / 2.0;
    if (n - tan0) > (tan1 - n) {
        tan1
    } else {
        tan0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::NotNan;
    use rand::{thread_rng, Rng as _};

    #[test]
    fn nearest_triangle_numbers() {
        let cases = [(1, 1), (16, 15), (14, 15), (1000, 990)];
        for (start, expect) in cases.iter() {
            assert_eq!(nearest_triangle_number(*start as f64), *expect as f64);
        }
    }

    #[test]
    fn debug_ratios() {
        const WEB_POW: f64 = 1.1;
        fn web_utility(ms: u64) -> f64 {
            let mut tracker = Performance::default();
            for _ in 0..1000 {
                tracker.add_query(Duration::from_millis(ms), Ok(()));
            }
            tracker.expected_utility(WEB_POW)
        }

        const UTILITY_PREFERENCE: f64 = 1.1;

        let a = web_utility(50); // 70%
        let b = web_utility(250); // 29%
        let c = web_utility(750); // 01%

        let s = a + b + c;

        println!("A: {}\nB: {}\nC: {}", a / s, b / s, c / s);
        println!("A / B: {}", a / b);
        println!("A / C: {}", a / c);

        let scores = vec![a, b, c];

        let max_utility = scores
            .iter()
            .map(|v| NotNan::new(*v).unwrap())
            .max()
            .unwrap()
            .into_inner();

        use std::collections::HashMap;
        let mut selections = HashMap::<usize, usize>::new();
        for _ in 0..100000 {
            let mut utility_cutoff: f64 = thread_rng().gen();
            utility_cutoff = max_utility * (1.0 - utility_cutoff.powf(UTILITY_PREFERENCE));
            use crate::WeightedSample;
            let mut selected = WeightedSample::new();
            let scores = scores.iter().filter(|score| **score >= utility_cutoff);
            for (i, _) in scores.enumerate() {
                selected.add(i, 1.0);
            }
            let selected = selected.take().unwrap();
            *selections.entry(selected).or_default() += 1;
        }

        let mut selections: Vec<_> = selections.into_iter().collect();
        selections.sort();
        let selections: Vec<_> = selections.iter().map(|s| s.1 as f64 / 100000.0).collect();
        dbg!(selections);
    }
}
