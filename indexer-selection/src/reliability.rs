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
        self.penalty = self.penalty + p as f64;
    }
}

impl ExpectedValue for Reliability {
    // https://www.desmos.com/calculator/gspottbqp7
    fn expected_value(&self) -> f64 {
        let successful_queries = self.successful_queries + 0.1;
        let total_queries = successful_queries + self.failed_queries;
        let p_success = successful_queries / total_queries;
        let p_penalty = self.penalty / total_queries;
        p_success / (p_penalty + 1.0)
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
