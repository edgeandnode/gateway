// This module uses "automated volume discounting" which gives each API key user a different
// query budget based on their estimated global query volume. The generalized equation for the
// price is:
//
//     scale / ((volume + offset) ^ discount)
//
// Where:
//  * volume: The estimated count of queries over a 30 day period
//  * offset: A query volume increase so that the differences are less extreme toward 0
//  * discount: How much discount to apply. As this approaches 0.0, all queries are priced
//    the same regardless of volume. As this approaches 1.0, all API keys will pay the same
//    amount regardless of volume.
//  * scale: Moves the price up or down linearly.
//
// The magic values chosen were based off of 30 days hosted service volume taken on Feb 17, 2022
// then tweaking until it looked like a fair distribution.

use crate::{indexer_selection::decay::*, prelude::*};
use chrono::{TimeZone as _, Utc};

use super::clock::*;

#[derive(Clone)]
pub struct QueryBudgetFactors {
    pub scale: f64,
    pub discount: f64,
    pub processes: f64,
}

fn budget(volume: f64, factors: &QueryBudgetFactors) -> f64 {
    const OFFSET: f64 = 500.0;

    // TODO: This is a temporary hack to avoid prices shooting up overnight.
    // This should be removed after May 30.
    let start = Utc.ymd(2022, 5, 23);
    let end = Utc.ymd(2022, 5, 30);
    let min_scale = 0.2;
    let today = Utc::today();
    let scale = if (start <= today) && (today < end) && (factors.scale > min_scale) {
        let step = (factors.scale - min_scale) / (end - start).num_days() as f64;
        min_scale + (step * (today - start).num_days() as f64)
    } else {
        factors.scale
    };

    scale / (volume * factors.processes + OFFSET).powf(factors.discount)
}

// For each bucket:
// Know the time-in-bucket
// Know the number of queries
// Move time and queries over to new buckets
#[derive(Default, Debug)]
struct QueryVolume {
    time_elapsed: Duration,
    num_queries: f64,
}

impl_struct_decay!(QueryVolume {
    time_elapsed,
    num_queries
});

#[derive(Debug)]
pub struct VolumeEstimator<C = SystemClock> {
    history: FastDecayBuffer<QueryVolume>,
    last_time: Instant,
    clock: C,
}

impl Default for VolumeEstimator {
    fn default() -> Self {
        Self::new(SystemClock)
    }
}

impl<C> VolumeEstimator<C>
where
    C: Clock,
{
    pub fn new(clock: C) -> Self {
        Self {
            last_time: clock.now(),
            history: FastDecayBuffer::new(),
            clock,
        }
    }

    // This must be called on a regular interval. The unit tests are assuming
    // 2 minutes.
    pub fn decay(&mut self) {
        let next = self.clock.now();
        let prev = self.last_time;
        self.last_time = next;
        self.history.current_mut().time_elapsed += next - prev;
        self.history.decay();
    }

    // Adds the queries and gives a budget for them.
    pub fn budget_for_queries(&mut self, count: u64, factors: &QueryBudgetFactors) -> f64 {
        let count = count as f64;
        self.history.current_mut().num_queries += count;
        budget(self.monthly_volume_estimate(), factors) * count
    }

    fn monthly_volume_estimate(&self) -> f64 {
        let mut elapsed_time = self.clock.now() - self.last_time;
        let mut queries = 0.0;
        for frame in self.history.frames() {
            elapsed_time += frame.time_elapsed;
            queries += frame.num_queries;
        }

        // Scale to 30 days
        let scale = 60.0 * 60.0 * 24.0 * 30.0;
        let elapsed_time = elapsed_time.as_secs_f64();

        queries * scale / elapsed_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FACTORS: QueryBudgetFactors = QueryBudgetFactors {
        scale: 3.1,
        discount: 0.6,
        processes: 1.0,
    };

    fn budget(volume: f64) -> f64 {
        super::budget(volume, &FACTORS)
    }

    #[track_caller]
    fn assert_approx(expected: f64, actual: f64, within: f64) {
        assert!((actual - expected).abs() <= within);
    }

    #[test]
    fn stable_volume() {
        let mut clock = MockClock::new();
        let mut estimate = VolumeEstimator::new(clock.clone());

        let factors = QueryBudgetFactors {
            processes: 10.0,
            ..FACTORS
        };

        // Over a long period, do 2 queries per second and verify
        // that the 30 day estimate is 5184000 across multiple delays
        const COUNT: f64 = 2.0 * 21600.0 * 120.0;
        for _ in 0..50 {
            for _ in 0..120 {
                clock.advance_time(Duration::from_secs(1));
                // Very precise, correct within < 1 query.
                assert_approx(
                    COUNT * super::budget(COUNT, &factors),
                    COUNT * estimate.budget_for_queries(2, &factors) / 2.0,
                    super::budget(COUNT, &factors),
                );
            }
            estimate.decay();
        }
    }

    #[test]
    fn sine_volume() {
        let mut clock = MockClock::new();
        let mut estimate = VolumeEstimator::new(clock.clone());

        // Show that a stable oscillating traffic has low variance
        // when looking at the estimate.
        let mut elapsed = 0.0f64;
        for _ in 0..100 {
            for _ in 0..1000 {
                for _ in 0..120 {
                    elapsed += 1.0;
                    clock.advance_time(Duration::from_secs(1));
                    // sin is -1 .. 1, so the range here is 100.0 .. 200.0
                    let queries = ((elapsed / 1000.0).sin() + 3.0) * 50.0;
                    estimate.budget_for_queries(queries as u64, &FACTORS);
                }
                estimate.decay();
            }
            let daily_estimate = estimate.monthly_volume_estimate() / 30.0;
            // The center of the range is 12,960,000.
            // The QPS oscillates at +- 33%
            // But, the estimate is within 2% on each iteration,
            // and is sometimes much closer. Of course, that means the
            // total error is less than 2% as well.
            assert_approx(12960000.0, daily_estimate, 250000.0);
        }
    }

    #[test]
    fn volume_increase() {
        let mut clock = MockClock::new();
        let mut estimate = VolumeEstimator::new(clock.clone());

        // Over a month, do 1 queries per minute. This is "testing"
        for _ in 0..21600 {
            clock.advance_time(Duration::from_secs(120));
            estimate.budget_for_queries(2, &FACTORS);
            estimate.decay();
        }

        // Now in "prod", do 20 queries per second. An increase of 1200x
        let frames = 30u64 * 24 * 30; // 30 days, 24 hours per day, 30 2 minute intervals per hour.
        let per_frame = 2400u64; // 2400 queries in 2 minutes is 20 per second
        let mut spend = 0.0;

        for _ in 0..frames {
            for _ in 0..per_frame {
                clock.advance_time(Duration::from_secs_f64(0.05));
                spend += estimate.budget_for_queries(1, &FACTORS);
            }
            estimate.decay();
        }

        let queries = (frames * per_frame) as f64;
        // If we knew a-priori what the volume would be, we may have set this budget.
        let should_spend = budget(queries) * queries;

        // Show that over 30 days this large increase of query volume was priced
        // more or less appropriately (within 3%). By the 8th day, the
        // budget has shifted from 0.0050870 to 0.0000729 (70x decrease), where it
        // remains stable. So, on the next month the billing would be perfect.
        assert!(spend > should_spend);
        assert!(spend < (should_spend * 1.03));
    }
}
