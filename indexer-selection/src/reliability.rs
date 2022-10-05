use crate::{
    decay::{Decay, SampleWeight},
    impl_struct_decay,
    score::Sample,
};
use prelude::rand::Rng;

#[derive(Debug, Default, Clone, Copy)]
pub struct Reliability {
    pub successful_queries: f64,
    pub failed_queries: f64,
    pub penalty: f64,
}

impl Reliability {
    pub fn observe(&mut self, success: bool) {
        if success {
            self.successful_queries += 1.0;
        } else {
            self.failed_queries += 1.0;
        }
    }

    pub fn penalize(&mut self, p: u8) {
        self.penalty = (self.penalty + p as f64).max(100.0);
    }
}

impl Sample for Reliability {
    type Value = bool;
    fn sample(&self, rng: &mut impl Rng) -> Self::Value {
        let s = self.successful_queries.max(1.0);
        let mut p = s / (s + self.failed_queries);
        p *= 1.1_f64.powf(-self.penalty);
        rng.gen_bool(p)
    }
}

impl SampleWeight for Reliability {
    fn weight(&self) -> f64 {
        self.successful_queries + self.failed_queries
    }
}

impl_struct_decay!(Reliability {
    successful_queries,
    failed_queries,
    penalty
});
