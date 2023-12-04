use std::time::Duration;

use eventuals::{Eventual, EventualWriter};
use indexer_selection::decay::DecayBuffer;
use prelude::*;
use tokio::time::interval;
use tokio::{select, spawn, sync::mpsc};

use crate::metrics::METRICS;

pub struct Budgeter {
    pub feedback: mpsc::UnboundedSender<USD>,
    query_fees_target: USD,
    budget_limit: Eventual<USD>,
}

impl Budgeter {
    pub fn new(query_fees_target: USD) -> Self {
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel();
        let (mut budget_limit_tx, budget_limit_rx) = Eventual::new();
        budget_limit_tx.write(query_fees_target);
        Actor::create(feedback_rx, budget_limit_tx, query_fees_target);
        Self {
            feedback: feedback_tx,
            query_fees_target,
            budget_limit: budget_limit_rx,
        }
    }

    pub fn budget(&self, candidate_fees: &[USD]) -> USD {
        let budget_min = USD(UDecimal18::try_from(10e-6).unwrap());
        let budget_max = USD(self.query_fees_target.0 * UDecimal18::from(2));

        let budget_limit = self
            .budget_limit
            .value_immediate()
            .unwrap_or(self.query_fees_target);
        let max_fee = candidate_fees.iter().max().cloned().unwrap_or_default();
        max_fee.max(budget_limit).clamp(budget_min, budget_max)
    }
}

struct Actor {
    feedback: mpsc::UnboundedReceiver<USD>,
    budget_limit: EventualWriter<USD>,
    controller: Controller,
}

impl Actor {
    fn create(
        feedback: mpsc::UnboundedReceiver<USD>,
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

    fn feedback(&mut self, fees: USD) {
        self.controller.add_recent_fees(fees);
    }

    fn revise_budget(&mut self) {
        if self.controller.recent_count == 0 {
            return;
        }
        let budget_limit = self.controller.control_variable();
        tracing::debug!(?budget_limit);
        self.budget_limit.write(budget_limit);
    }
}

struct Controller {
    query_fees_target: USD,
    recent_fees: USD,
    recent_count: u64,
    error_history: DecayBuffer<f64, 6, 4>,
}

impl Controller {
    fn new(query_fees_target: USD) -> Self {
        Self {
            query_fees_target,
            recent_fees: USD(UDecimal18::from(0)),
            recent_count: 0,
            error_history: DecayBuffer::default(),
        }
    }

    fn add_recent_fees(&mut self, fees: USD) {
        self.recent_fees = USD(self.recent_fees.0 + fees.0);
        self.recent_count += 1;
    }

    fn control_variable(&mut self) -> USD {
        // See the following link if you're unfamiliar with PID controllers:
        // https://en.wikipedia.org/wiki/Proportional%E2%80%93integral%E2%80%93derivative_controller
        let target = f64::from(self.query_fees_target.0);
        let process_variable = f64::from(self.recent_fees.0) / self.recent_count.max(1) as f64;
        tracing::debug!(avg_fees = process_variable);
        METRICS.avg_query_fees.set(process_variable);

        self.recent_fees = USD(UDecimal18::from(0));
        self.recent_count = 0;
        self.error_history.decay();
        let error = (target - process_variable) / target;
        *self.error_history.current_mut() += error;

        let i: f64 = self.error_history.frames().iter().sum();
        let k_i = 0.2;
        let control_variable = (i * k_i) * target;
        USD(UDecimal18::try_from(target + control_variable).unwrap_or_default())
    }
}
