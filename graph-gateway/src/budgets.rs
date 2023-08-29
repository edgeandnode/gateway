use std::collections::HashMap;

use eventuals::EventualExt;
use indexer_selection::{
    decay::{Decay, FastDecayBuffer},
    impl_struct_decay,
};
use prelude::USD;
use tokio::{
    sync::{Mutex, RwLock},
    time::{Duration, Instant},
};
use toolshed::thegraph::DeploymentId;

pub struct Budgeter {
    query_volume: RwLock<HashMap<DeploymentId, Mutex<VolumeEstimator>>>,
}

impl Budgeter {
    pub fn new() -> &'static Self {
        let query_volume = RwLock::default();
        let budgeter: &'static Self = Box::leak(Self { query_volume }.into());

        eventuals::timer(Duration::from_secs(120))
            .pipe_async(|_| async {
                let query_volume = budgeter.query_volume.read().await;
                for estimator in query_volume.values() {
                    estimator.lock().await.decay(Instant::now());
                }
            })
            .forever();

        budgeter
    }

    pub async fn estimate_budget(&self, deployment: &DeploymentId, query_count: u64) -> USD {
        let monthly_volume = self.estimate_volume(deployment, query_count).await;

        // We're currenty "automated volume discounting" which assigns each deployment a different
        // query budget based on their estimated global query volume. The generalized equation for
        // the price is:
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
        // The magic values chosen were based off of 30 days hosted service volume taken on
        // 2022-02-17, and then tweaking until it looked like a fair distribution.
        //
        // TODO: target global average price per query
        const SCALE: f64 = 1.4;
        const DISCOUNT: f64 = 0.49;
        const OFFSET: u64 = 500;
        const PROCESSES: u64 = 4;
        let budget = SCALE / ((monthly_volume * PROCESSES + OFFSET) as f64).powf(DISCOUNT);
        USD::try_from(budget).unwrap()
    }

    async fn estimate_volume(&self, deployment: &DeploymentId, query_count: u64) -> u64 {
        // Note that we are making no attempt to ever remove a volume estimator when the deployment
        // is no longer used.

        // optimistic read & update
        let query_volume = self.query_volume.read().await;
        if let Some(estimator) = query_volume.get(deployment) {
            let mut estimator = estimator.lock().await;
            estimator.add_queries(query_count);
            return estimator.monthly_volume_estimate(Instant::now()) as u64;
        }
        drop(query_volume);
        // insert with write lock (note that multiple writers might get here)
        let mut query_volume = self.query_volume.write().await;
        query_volume
            .entry(*deployment)
            .or_insert_with(|| VolumeEstimator::new(Instant::now()).into());
        drop(query_volume);
        // read & update
        let query_volume = self.query_volume.read().await;
        let mut estimator = query_volume.get(deployment).unwrap().lock().await;
        estimator.add_queries(query_count);
        estimator.monthly_volume_estimate(Instant::now()) as u64
    }
}

struct VolumeEstimator {
    history: FastDecayBuffer<QueryVolume>,
    last_time: Instant,
}

#[derive(Default)]
struct QueryVolume {
    time_elapsed: Duration,
    num_queries: f64,
}

impl_struct_decay!(QueryVolume {
    time_elapsed,
    num_queries
});

impl VolumeEstimator {
    pub fn new(now: Instant) -> Self {
        Self {
            last_time: now,
            history: FastDecayBuffer::new(),
        }
    }

    // This must be called on a regular interval. The unit tests are assuming
    // 2 minutes.
    pub fn decay(&mut self, now: Instant) {
        let prev = self.last_time;
        self.last_time = now;
        self.history.current_mut().time_elapsed += now - prev;
        self.history.decay();
    }

    pub fn add_queries(&mut self, count: u64) {
        self.history.current_mut().num_queries += count as f64;
    }

    pub fn monthly_volume_estimate(&self, now: Instant) -> f64 {
        let mut elapsed_time = now - self.last_time;
        let mut queries = 0.0;
        for frame in self.history.frames() {
            elapsed_time += frame.time_elapsed;
            queries += frame.num_queries;
        }

        // Scale to 30 days
        let scale = 60.0 * 60.0 * 24.0 * 30.0;
        let elapsed_time = elapsed_time.as_secs_f64();

        (queries * scale) / elapsed_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn assert_approx(expected: f64, actual: f64, within: f64) {
        assert!((actual - expected).abs() <= within);
    }

    #[test]
    fn stable_volume() {
        let mut now = Instant::now();
        let mut estimate = VolumeEstimator::new(now);

        // Over a long period, do 2 queries per second and verify that the 30 day estimate is
        // 5184000 across multiple delays
        const COUNT: f64 = 2.0 * 21600.0 * 120.0;
        for _ in 0..50 {
            for _ in 0..120 {
                now += Duration::from_secs(1);
                estimate.add_queries(2);
                // Very precise, correct within < 1 query.
                assert_approx(estimate.monthly_volume_estimate(now), COUNT, 1.0);
            }
            estimate.decay(now);
        }
    }

    #[test]
    fn sine_volume() {
        let mut now = Instant::now();
        let mut estimate = VolumeEstimator::new(now);

        // Show that a stable oscillating traffic has low variance when looking at the estimate.
        let mut elapsed = 0.0_f64;
        for _ in 0..100 {
            for _ in 0..1000 {
                for _ in 0..120 {
                    now += Duration::from_secs(1);
                    elapsed += 1.0;
                    // sin is -1 .. 1, so the range here is 100.0 .. 200.0
                    let queries = ((elapsed / 1000.0).sin() + 3.0) * 50.0;
                    estimate.add_queries(queries as u64);
                }
                estimate.decay(now);
            }
            let daily_estimate = estimate.monthly_volume_estimate(now) / 30.0;
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
        let mut now = Instant::now();
        let mut estimate = VolumeEstimator::new(now);

        // Over a month, do 1 queries per minute. This is "testing"
        for _ in 0..21600 {
            now += Duration::from_secs(120);
            estimate.add_queries(2);
            estimate.decay(now);
        }
        // Now in "prod", do 20 queries per second. An increase of 1200x.
        // 30 days, 24 hours per day, 30 2 minute intervals per hour.
        let frames = 30_u64 * 24 * 30;
        // 2400 queries in 2 minutes is 20 per second.
        let per_frame = 2400_u64;
        for _ in 0..frames {
            for _ in 0..per_frame {
                now += Duration::from_secs_f64(0.05);
                estimate.add_queries(1);
            }
            estimate.decay(now);
        }

        let queries = (frames * per_frame) as f64;
        let estimation = estimate.monthly_volume_estimate(now);

        // Show that over 30 days this large increase of query volume was estimated more or less
        // appropriately (within 3%).
        assert!(estimation > queries);
        assert!(estimation < (queries * 1.03));
    }
}
