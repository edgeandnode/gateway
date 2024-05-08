use std::time::Duration;

use ordered_float::NotNan;
use tokio::{
    select, spawn,
    sync::{mpsc, watch},
    time::interval,
};

use crate::reporting::METRICS;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct USD(pub NotNan<f64>);

pub struct Budgeter {
    pub feedback: mpsc::UnboundedSender<USD>,
    pub query_fees_target: USD,
    pub min_indexer_fees: watch::Receiver<USD>,
}

impl Budgeter {
    pub fn new(query_fees_target: USD) -> Self {
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel();
        let (min_indexer_fees_tx, min_indexer_fees_rx) = watch::channel(query_fees_target);
        Actor::create(feedback_rx, min_indexer_fees_tx, query_fees_target);
        Self {
            feedback: feedback_tx,
            query_fees_target,
            min_indexer_fees: min_indexer_fees_rx,
        }
    }
}

struct Actor {
    feedback: mpsc::UnboundedReceiver<USD>,
    min_indexer_fees: watch::Sender<USD>,
    controller: Controller,
}

impl Actor {
    fn create(
        feedback: mpsc::UnboundedReceiver<USD>,
        min_indexer_fees: watch::Sender<USD>,
        query_fees_target: USD,
    ) {
        let mut actor = Actor {
            feedback,
            min_indexer_fees,
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
        let min_indexer_fees = self.controller.control_variable();
        tracing::debug!(?min_indexer_fees);
        if let Err(min_indexer_fees_send_err) = self.min_indexer_fees.send(min_indexer_fees) {
            tracing::error!(%min_indexer_fees_send_err);
        };
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
            recent_fees: USD(NotNan::default()),
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

        self.recent_fees = USD(NotNan::default());
        self.recent_count = 0;
        self.error_history.decay();
        let error = (target - process_variable) / target;
        *self.error_history.current_mut() += error;

        let i: f64 = self.error_history.frames().iter().sum();
        let k_i = 0.2;
        let control_variable = (i * k_i) * target;

        let min = NotNan::new(10e-6).unwrap_or_default();
        let max = self.query_fees_target.0;
        USD(NotNan::new(control_variable)
            .unwrap_or_default()
            .clamp(min, max))
    }
}

// TODO: the following is overkill now that it's only used here.

#[derive(Clone, Debug)]
struct DecayBuffer<T, const F: usize, const D: u16> {
    frames: [T; F],
}

trait Decay {
    fn decay(&mut self, prev: &Self, retain: f64, take: f64);
}

impl Decay for f64 {
    fn decay(&mut self, prev: &Self, retain: f64, take: f64) {
        *self = (*self * retain) + (prev * take);
    }
}

impl<T, const F: usize, const D: u16> Default for DecayBuffer<T, F, D>
where
    [T; F]: Default,
{
    fn default() -> Self {
        debug_assert!(F > 0);
        debug_assert!(D < 1000);
        Self {
            frames: Default::default(),
        }
    }
}

impl<T, const F: usize, const D: u16> DecayBuffer<T, F, D> {
    fn current_mut(&mut self) -> &mut T {
        &mut self.frames[0]
    }

    fn frames(&self) -> &[T] {
        &self.frames
    }
}

impl<T, const F: usize, const D: u16> DecayBuffer<T, F, D>
where
    T: Decay + Default,
{
    fn decay(&mut self) {
        // BQN: (1-1e¯3×d)×((1-4⋆-↕f)×⊢)+(«4⋆-↕f)×⊢
        // LLVM should be capable of constant folding & unrolling this loop nicely.
        // https://rust.godbolt.org/z/K13dj78Ge
        for i in (1..self.frames.len()).rev() {
            let retain = 1.0 - 4_f64.powi(-(i as i32));
            let take = 4_f64.powi(-(i as i32 - 1));
            let decay = 1.0 - 1e-3 * D as f64;
            let (cur, prev) = self.frames[..=i].split_last_mut().unwrap();
            cur.decay(prev.last().unwrap(), retain * decay, take * decay);
        }
        self.frames[0] = T::default();
    }
}
