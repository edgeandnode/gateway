use crate::{
    decay::{Decay, ISADecayBuffer},
    impl_struct_decay,
    score::ExpectedValue,
    utility::UtilityFactor,
    ConcaveUtilityParameters,
};
use prelude::*;
use std::f64::consts::E;

// https://www.desmos.com/calculator/y2t5704v6a
// 170cbcf3-db7f-404a-be13-2022d9142677
pub fn performance_utility(
    params: ConcaveUtilityParameters,
    probability: f64,
    latency_ms: u32,
) -> UtilityFactor {
    let sigmoid = |x: u32| 1.0 + E.powf(((x as f64).powf(params.a) - 400.0) / 300.0);
    UtilityFactor {
        utility: sigmoid(0) / sigmoid(latency_ms),
        weight: params.weight * probability,
    }
}

#[derive(Clone, Debug, Default)]
pub struct Performance {
    total_latency_ms: f64,
    count: f64,
}

impl Performance {
    pub fn observe(&mut self, duration: Duration) {
        self.total_latency_ms += duration.as_millis() as f64;
        self.count += 1.0;
    }
}

impl ExpectedValue for ISADecayBuffer<Performance> {
    fn expected_value(&self) -> f64 {
        let total_latency_ms = self.map(|p| p.total_latency_ms).sum::<f64>();
        let total_count = self.map(|p| p.count).sum::<f64>();
        (total_latency_ms + 0.1) / total_count.max(1.0)
    }
}

impl_struct_decay!(Performance {
    total_latency_ms,
    count
});
