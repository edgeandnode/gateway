use crate::{
    decay::{Decay, FrameWeight},
    impl_struct_decay,
    score::ExpectedValue,
};
use prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Performance {
    total_latency_ms: f64,
    count: f64,
}

impl_struct_decay!(Performance {
    total_latency_ms,
    count
});

impl Performance {
    pub fn observe(&mut self, duration: Duration) {
        self.total_latency_ms += duration.as_millis() as f64;
    }
}

impl ExpectedValue for Performance {
    fn expected_value(&self) -> f64 {
        (self.total_latency_ms + 0.1) / self.count.max(1.0)
    }
}

impl FrameWeight for Performance {
    fn weight(&self) -> f64 {
        self.count
    }
}
