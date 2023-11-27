use std::time::Duration;

use eventuals::{Eventual, EventualWriter};
use indexer_selection::decay::FastDecayBuffer;
use prelude::*;
use tokio::time::interval;
use tokio::{select, spawn, sync::mpsc};

use crate::metrics::METRICS;

pub struct Budgeter {
    pub feedback: mpsc::UnboundedSender<Feedback>,
    absolute_budget_limit: USD,
    budget_limit: Eventual<USD>,
}

pub struct Feedback {
    pub fees: USD,
    pub query_count: u64,
}

impl Budgeter {
    pub fn new(query_fees_target: USD) -> Self {
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel();
        let (mut budget_limit_tx, budget_limit_rx) = Eventual::new();
        budget_limit_tx.write(query_fees_target);
        Actor::create(feedback_rx, budget_limit_tx, query_fees_target);
        let absolute_budget_limit = USD(query_fees_target.0 * UDecimal18::from(10));
        Self {
            feedback: feedback_tx,
            absolute_budget_limit,
            budget_limit: budget_limit_rx,
        }
    }

    pub fn budget(&self, query_count: u64, candidate_fees: &[USD]) -> USD {
        let budget_limit = self
            .budget_limit
            .value_immediate()
            .unwrap_or(self.absolute_budget_limit);
        let max_fee = candidate_fees.iter().max().cloned().unwrap_or_default();
        let budget = max_fee.max(budget_limit).min(self.absolute_budget_limit);
        USD(budget.0 * UDecimal18::from(query_count as u128))
    }
}

struct Actor {
    feedback: mpsc::UnboundedReceiver<Feedback>,
    budget_limit: EventualWriter<USD>,
    controller: Controller,
}

impl Actor {
    fn create(
        feedback: mpsc::UnboundedReceiver<Feedback>,
        budget_limit: EventualWriter<USD>,
        query_fees_target: USD,
    ) {
        let mut actor = Actor {
            feedback,
            budget_limit,
            controller: Controller::new(query_fees_target),
        };
        let mut budget_timer = interval(Duration::from_secs(1));
        spawn(async move {
            loop {
                select! {
                    Some(msg) = actor.feedback.recv() => actor.feedback(msg),
                    _ = budget_timer.tick() => actor.revise_budget(),
                }
            }
        });
    }

    fn feedback(&mut self, feedback: Feedback) {
        self.controller
            .add_queries(feedback.fees, feedback.query_count);
    }

    fn revise_budget(&mut self) {
        if self.controller.recent_query_count == 0 {
            return;
        }
        let budget_limit = USD(self.controller.control_variable());
        tracing::debug!(?budget_limit);
        self.budget_limit.write(budget_limit);
    }
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
        let mut error_history = FastDecayBuffer::default();
        *error_history.current_mut() = target_query_fees.0.into();
        Self {
            target_query_fees,
            recent_fees: USD(UDecimal18::from(0)),
            recent_query_count: 0,
            error_history,
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
        let k_i = 1.2;
        let correction = UDecimal18::try_from(i * k_i).unwrap_or_default();
        self.target_query_fees.0 + correction
    }
}

#[cfg(test)]
mod tests {
    use indexer_selection::test_utils::assert_within;

    use super::*;

    #[test]
    fn controller() {
        fn test_controller(
            controller: &mut Controller,
            process_variable_multiplier: f64,
            tolerance: f64,
        ) {
            let setpoint: f64 = controller.target_query_fees.0.into();
            let mut process_variable = 0.0;
            for i in 0..20 {
                let control_variable: f64 = controller.control_variable().into();
                process_variable = control_variable * process_variable_multiplier;
                println!(
                    "{i:02} SP={setpoint:.6}, PV={:.8}, CV={:.8}",
                    process_variable, control_variable,
                );
                controller.add_queries(USD(UDecimal18::try_from(process_variable).unwrap()), 1);
            }
            assert_within(process_variable, setpoint, tolerance);
        }

        for setpoint in [10e-6, 20e-6, 50e-6] {
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
