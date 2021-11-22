// The design comes from https://www.notion.so/thegraph/Indexer-Selection-Algorithm-fc95c7a0b11c436288c82094002d4c3d
// The design boils down to these steps:
//    Model a curve that represent the expected time for a query
//    Transform the "expected time" into a notion of "performance" (the inverse of expected time)
//    Multiply this performance curve with the utility curve
//    Integrate over a range to get the expected utility.
//
// The observation in the design below is that you get the same result
// by just averaging the utility of all queries in the relevant window.
//
// The simplest possible thing would be to convert all queries to utility,
// aggregate utilities and count, then use that to calculate the average utility.
// The problem with that approach is that it doesn't allow us to parameterize
// the utility function.
//
// So instead we take only a slightly more complicated approach.
// First, we convert all durations into a quantized performance.
// The quantized performance serves as a key to a bucket of queries
// with nearly the same performance. This limits the number of
// performance->utility conversions that we need to calculate the
// expected performance which enables making the utility function
// a parameter without obscene loss to performance.

use crate::{
    indexer_selection::utility::{concave_utility, SelectionFactor},
    prelude::*,
};
use ordered_float::NotNan;

#[derive(Clone, Debug, Default)]
pub struct Performance {
    performance: Vec<f64>,
    count: Vec<f64>,
}

impl Performance {
    fn quantized_performance(duration: Duration) -> f64 {
        // Get nearest triangle number of quantized duration, then convert to performance.
        // The idea here is to quantize with variable precision to limit bucket count,
        // but also have a reasonable number of buckets no matter what scale you care about.
        //
        // 6 buckets between 200ms - 300ms (a 100ms delta)
        // 6 buckets between 17s - 18s (a 1000ms delta for the same number of buckets as above)
        // 244 buckets between 0s-30s... or about the most we'll ever have to keep.
        //
        // There's probably a smarter way, but this seems reasonable if you squint.
        // The tests seemed in practice to create around 100 buckets after 100,000 queries,
        // which seems about what you would hope for to approximate a curve. There was variation
        // because of different values for std_dev
        let duration_ms = duration.as_secs_f64() * 1000.0;
        // Subtracting a small amount of here moves the inflection of the curve so
        // that changes after this point are more noticeable. This makes a big
        // difference to how often a 2000ms indexer is selected compared to a 200ms
        // indexer (for the better).
        let duration_ms = duration_ms - 35.0;
        let duration_ms = nearest_triangle_number(duration_ms.max(1.0));
        let performance = 1000.0 / duration_ms;

        // SAFETY: We force performance to be NotNan here. duration_ms is >=
        // 1.0 and NotNan because of the clamp (above) and implementation of Duration.
        // Therefore performance is <= 1000.0 and NonNan.
        // See also 47632ed6-4dcc-4b39-b064-c0ca01560626
        // I think I would be comfortable changing this assert
        // to a debug_assert.
        assert!(!performance.is_nan());

        performance
    }

    pub fn add_successful_query(&mut self, duration: Duration) {
        let performance = Self::quantized_performance(duration);
        // Safety: Performance is NotNan. See also 47632ed6-4dcc-4b39-b064-c0ca01560626
        match unsafe {
            self.performance
                .binary_search_by_key(&NotNan::new_unchecked(performance), |a| {
                    NotNan::new_unchecked(*a)
                })
        } {
            Ok(index) => self.count[index] += 1.0,
            Err(index) => {
                self.performance.insert(index, performance);
                self.count.insert(index, 1.0)
            }
        }
    }

    pub fn expected_utility(&self, u_a: f64) -> SelectionFactor {
        let mut agg_count = 0.0;
        let mut agg_utility = 0.0;
        for (count, performance) in self.iter() {
            agg_count += count;
            agg_utility += count * concave_utility(performance, u_a);
        }

        if agg_count == 0.0 {
            SelectionFactor::zero()
        } else {
            SelectionFactor {
                utility: agg_utility / agg_count,
                // This weight gives about 85% confidence after 10 samples
                // We would like more samples, but the query volume per indexer/deployment
                // pair is so low that it otherwise takes a very long time to converge.
                weight: concave_utility(agg_count, 0.19),
            }
        }
    }

    fn iter<'a>(&'a self) -> impl 'a + Iterator<Item = (f64, f64)> {
        self.count
            .iter()
            .cloned()
            .zip(self.performance.iter().cloned())
    }

    pub fn decay(&mut self, retain: f64) {
        for count in self.count.iter_mut() {
            *count *= retain;
        }
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
    use crate::indexer_selection::test_utils::*;
    use rand::{thread_rng, Rng as _};
    use rand_distr::Normal;

    // "Web utility" - based on percentage of users that will leave a site after an amount of time.
    const WEB_UTIL: f64 = 0.01;

    // For the use case of analytics where requests may be larger, parallel, data processing
    // needs to occur, and we are prioritizing other features like price efficiency instead.
    // I pretty much just made this number up though after checking a couple of values to see what fit.
    const ANALYTICS_UTIL: f64 = 4.0;

    /// Add a bunch of queries with randomized duration according to a performance profile.
    /// Check the utility, given some A.
    /// Verify that the expected utility is within a range.
    fn uniform_test(mean: f64, std_dev: f64, a_u: f64, expected: f64, tolerance: f64) {
        let mut tracker = Performance::default();
        let dist = Normal::new(mean, std_dev).unwrap();
        for _ in 0..10000 {
            let duration = thread_rng().sample(dist);
            let duration = Duration::from_millis(duration.max(0.0) as u64);
            tracker.add_successful_query(duration)
        }

        let utility = tracker.expected_utility(a_u);
        assert_within(utility, expected, tolerance);
        tracker.decay(0.75);
    }

    #[test]
    fn nearest_triangle_numbers() {
        let cases = [(1, 1), (16, 15), (14, 15), (1000, 990)];
        for (start, expect) in cases.iter() {
            assert_eq!(nearest_triangle_number(*start as f64), *expect as f64);
        }
    }

    #[test]
    fn poor_performance_web() {
        uniform_test(10000.0, 2000.0, WEB_UTIL, 0.001, 0.01);
    }

    #[test]
    fn mediocre_performance_web() {
        uniform_test(2000.0, 250.0, WEB_UTIL, 0.005, 0.01);
    }

    #[test]
    fn good_performance_web() {
        uniform_test(200.0, 50.0, WEB_UTIL, 0.067, 0.01);
    }

    #[test]
    fn great_performance_web() {
        uniform_test(50.0, 10.0, WEB_UTIL, 0.57, 0.01);
    }

    #[test]
    fn bad_performance_analytics() {
        uniform_test(100000.0, 10000.0, ANALYTICS_UTIL, 0.04, 0.01);
    }

    #[test]
    fn good_performance_analytics() {
        uniform_test(5000.0, 1000.0, ANALYTICS_UTIL, 0.56, 0.02);
    }

    #[test]
    fn great_performance_analytics() {
        uniform_test(1000.0, 1000.0, ANALYTICS_UTIL, 0.94, 0.02);
    }

    #[test]
    fn debug_ratios() {
        fn web_utility(ms: u64) -> f64 {
            let mut tracker = Performance::default();
            tracker.add_successful_query(Duration::from_millis(ms));
            tracker.expected_utility(WEB_UTIL).utility
        }

        // What we want to see is a high preference for values
        // that are low, but a strong preference between low values.
        // This doesn't really achieve that as well as I might prefer.
        // I don't think it's going to get where I might like just by
        // tweaking a few numbers.

        let a = web_utility(50);
        let b = web_utility(250);
        let c = web_utility(2000);
        let s = a + b + c;

        println!("A: {}\nB: {}\nC: {}", a / s, b / s, c / s);
        println!("A / B: {}", a / b);
        println!("A / C: {}", a / c);
    }
}
