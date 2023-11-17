use std::collections::HashMap;

use eventuals::{Eventual, EventualWriter, Ptr};
use indexer_selection::{
    decay::{Decay, FastDecayBuffer},
    impl_struct_decay,
};
use prelude::{UDecimal18, USD};
use tokio::{
    select, spawn,
    sync::mpsc,
    time::{interval, Duration, Instant},
};
use toolshed::thegraph::DeploymentId;

use crate::metrics::METRICS;

/// This 10e-6 number comes some back-of-the-napkin calculations on what we expect is the minimum
/// fee an indexer should be paid per query, based on hosted service costs attributable to serving
/// queries in June 2023. Now we are using it as the maximum discount instead of the minimum budget.
const MAX_DISCOUNT_USD: f64 = 10e-6;

pub struct Budgeter {
    pub feedback: mpsc::UnboundedSender<Feedback>,
    pub budgets: Eventual<Ptr<HashMap<DeploymentId, USD>>>,
    query_fees_target: USD,
}

pub struct Feedback {
    pub deployment: DeploymentId,
    pub fees: USD,
    pub query_count: u64,
}

impl Budgeter {
    pub fn new(query_fees_target: USD) -> Self {
        assert!(f64::from(query_fees_target.0) >= MAX_DISCOUNT_USD);
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel();
        let (budgets_tx, budgets_rx) = Eventual::new();
        Actor::create(feedback_rx, budgets_tx, query_fees_target);
        Self {
            feedback: feedback_tx,
            budgets: budgets_rx,
            query_fees_target,
        }
    }

    pub fn budget(&self, deployment: &DeploymentId, query_count: u64) -> USD {
        let budget = self
            .budgets
            .value_immediate()
            .and_then(|budgets| budgets.get(deployment).copied())
            .unwrap_or(self.query_fees_target);
        USD(budget.0 * UDecimal18::from(query_count as u128))
    }
}

struct Actor {
    feedback: mpsc::UnboundedReceiver<Feedback>,
    budgets: EventualWriter<Ptr<HashMap<DeploymentId, USD>>>,
    volume_estimators: HashMap<DeploymentId, VolumeEstimator>,
    controller: Controller,
}

impl Actor {
    fn create(
        feedback: mpsc::UnboundedReceiver<Feedback>,
        budgets: EventualWriter<Ptr<HashMap<DeploymentId, USD>>>,
        query_fees_target: USD,
    ) {
        let mut actor = Actor {
            feedback,
            budgets,
            volume_estimators: HashMap::default(),
            controller: Controller::new(query_fees_target),
        };
        let mut decay_timer = interval(Duration::from_secs(120));
        let mut budget_timer = interval(Duration::from_secs(1));
        spawn(async move {
            loop {
                select! {
                    _ = decay_timer.tick() => actor.decay(),
                    Some(msg) = actor.feedback.recv() => actor.feedback(msg),
                    _ = budget_timer.tick() => actor.revise_budget(),
                }
            }
        });
    }

    fn decay(&mut self) {
        let now = Instant::now();
        for estimator in self.volume_estimators.values_mut() {
            estimator.decay(now);
        }
    }

    fn feedback(&mut self, feedback: Feedback) {
        self.controller
            .add_queries(feedback.fees, feedback.query_count);
        self.volume_estimators
            .entry(feedback.deployment)
            .or_insert_with(|| VolumeEstimator::new(Instant::now()))
            .add_queries(feedback.query_count);
    }

    fn revise_budget(&mut self) {
        if self.controller.recent_query_count == 0 {
            return;
        }
        let target = self.controller.target_query_fees;
        let control_variable = self.controller.control_variable();
        tracing::debug!(budget_control_variable = ?control_variable);
        let now = Instant::now();
        let budgets = self
            .volume_estimators
            .iter()
            .map(|(deployment, volume_estimator)| {
                let volume = volume_estimator.monthly_volume_estimate(now) as u64;
                let mut budget = volume_discount(volume, target).0 * control_variable;
                // limit budget to 100x target
                budget = budget.min(target.0 * UDecimal18::from(100));
                (*deployment, USD(budget))
            })
            .collect();

        self.budgets.write(Ptr::new(budgets));
    }
}

fn volume_discount(monthly_volume: u64, target: USD) -> USD {
    // Discount the budget, based on a generalized logistic function. We apply little to no discount
    // between 0 and ~10e3 queries per month. And we limit the discount to 10E-6 USD.
    // https://www.desmos.com/calculator/whtakt50sa
    let b_max: f64 = target.0.into();
    let b_min = b_max - MAX_DISCOUNT_USD;
    let m: f64 = 1e6;
    let z: f64 = 0.45;
    let v = monthly_volume as f64;
    let budget = b_min + ((b_max - b_min) * m.powf(z)) / (v + m).powf(z);
    USD(budget.try_into().unwrap_or_default())
}

/// State for the control loop targeting `recent_query_fees`.
struct Controller {
    target_query_fees: USD,
    recent_fees: USD,
    recent_query_count: u64,
    error_history: FastDecayBuffer<f64>,
}

impl Controller {
    fn new(target_query_fees: USD) -> Self {
        Self {
            target_query_fees,
            recent_fees: USD(UDecimal18::from(0)),
            recent_query_count: 0,
            error_history: FastDecayBuffer::default(),
        }
    }

    fn add_queries(&mut self, fees: USD, query_count: u64) {
        self.recent_fees = USD(self.recent_fees.0 + fees.0);
        self.recent_query_count += query_count;
    }

    fn control_variable(&mut self) -> UDecimal18 {
        // See the following link if you're unfamiliar with PID controllers:
        // https://en.wikipedia.org/wiki/Proportional%E2%80%93integral%E2%80%93derivative_controller
        let process_variable =
            f64::from(self.recent_fees.0) / self.recent_query_count.max(1) as f64;
        METRICS.avg_query_fees.set(process_variable);

        self.recent_fees = USD(UDecimal18::from(0));
        self.recent_query_count = 0;
        self.error_history.decay();
        let error = f64::from(self.target_query_fees.0) - process_variable;
        *self.error_history.current_mut() = error;

        let i: f64 = self.error_history.frames().iter().sum();
        let k_i = 3e4;
        UDecimal18::from(1) + UDecimal18::try_from(i * k_i).unwrap_or_default()
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
    use indexer_selection::test_utils::assert_within;

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

    #[test]
    fn controller() {
        fn test_controller(
            controller: &mut Controller,
            process_variable_multiplier: f64,
            tolerance: f64,
        ) {
            let setpoint: f64 = controller.target_query_fees.0.into();
            let mut process_variable = 0.0;
            for i in 0..30 {
                let control_variable: f64 = controller.control_variable().into();
                process_variable = f64::from(controller.target_query_fees.0)
                    * process_variable_multiplier
                    * control_variable;
                println!(
                    "{i:02} SP={setpoint:.6}, PV={:.8}, CV={:.8}",
                    process_variable, control_variable,
                );
                controller.add_queries(USD(UDecimal18::try_from(process_variable).unwrap()), 1);
            }
            assert_within(process_variable, setpoint, tolerance);
        }

        for setpoint in [20e-6, 40e-6] {
            let setpoint = USD(UDecimal18::try_from(setpoint).unwrap());
            let mut controller = Controller::new(setpoint);
            test_controller(&mut controller, 0.2, 1e-6);
            let mut controller = Controller::new(setpoint);
            test_controller(&mut controller, 0.6, 1e-6);
            let mut controller = Controller::new(setpoint);
            test_controller(&mut controller, 0.8, 1e-6);

            let mut controller = Controller::new(setpoint);
            test_controller(&mut controller, 0.2, 1e-6);
            test_controller(&mut controller, 0.6, 1e-6);
            test_controller(&mut controller, 0.7, 1e-6);
        }
    }
}
