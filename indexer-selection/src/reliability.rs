use crate::{
    decay::{Decay, ISADecayBuffer},
    impl_struct_decay,
    score::ExpectedValue,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct Reliability {
    successful_queries: f64,
    failed_queries: f64,
    penalties: f64,
}

impl Reliability {
    pub fn observe(&mut self, success: bool) {
        if success {
            self.successful_queries += 1.0;
        } else {
            self.failed_queries += 1.0;
        }
    }

    pub fn penalize(&mut self) {
        self.penalties += 1.0;
    }
}

impl ExpectedValue for ISADecayBuffer<Reliability> {
    // https://www.desmos.com/calculator/gspottbqp7
    fn expected_value(&self) -> f64 {
        let successful_queries = self.map(|r| r.successful_queries).sum::<f64>() + 0.1;
        let total_queries = successful_queries + self.map(|r| r.failed_queries).sum::<f64>();
        let p_success = successful_queries / total_queries;
        let p_penalty = self.map(|r| r.penalties).sum::<f64>() / total_queries;
        p_success / (p_penalty + 1.0)
    }
}

impl_struct_decay!(Reliability {
    successful_queries,
    failed_queries,
    penalties
});
