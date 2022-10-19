use crate::{
    decay::{Decay, FrameWeight},
    impl_struct_decay,
    score::ExpectedValue,
    utility::UtilityFactor,
    ConcaveUtilityParameters,
};
use prelude::*;
use std::f64::consts::E;

// https://www.desmos.com/calculator/hegcczzalf
// 170cbcf3-db7f-404a-be13-2022d9142677
pub fn performance_utility(params: ConcaveUtilityParameters, latency_ms: u32) -> UtilityFactor {
    let sigmoid = |x: u32| 1.0 + E.powf(((x as f64).powf(params.a) - 400.0) / 300.0);
    UtilityFactor {
        utility: sigmoid(0) / sigmoid(latency_ms),
        weight: params.weight,
    }
}

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
