use crate::{
    decay::{Decay, FrameWeight},
    impl_struct_decay,
    score::ExpectedValue,
};

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

impl ExpectedValue for Reliability {
    fn expected_value(&self) -> f64 {
        let s = self.successful_queries + 0.1;
        let p = s / (s + self.failed_queries);
        p * 1.1_f64.powf(-self.penalty)
    }
}

impl FrameWeight for Reliability {
    fn weight(&self) -> f64 {
        self.successful_queries + self.failed_queries
    }
}

impl_struct_decay!(Reliability {
    successful_queries,
    failed_queries,
    penalty
});
