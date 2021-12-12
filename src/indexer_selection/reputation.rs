use crate::indexer_selection::decay::Decay;

// TODO: Other factors like how long the indexer has been in the network.
// Because reliability (which is what is captured here now) is useful on it's own, it may be useful
// to separate this into it's own utility rather than figure out how to combine that with other
// "reputation" factors which are much more subjective.

#[derive(Debug, Default, Clone)]
pub struct Reputation {
    successful_queries: f64,
    failed_queries: f64,
    penalties: f64,
}

impl Decay for Reputation {
    fn expected_utility(&self, _u_a: f64) -> f64 {
        let total_queries = self.successful_queries + self.failed_queries;
        if total_queries == 0.0 {
            return 0.0;
        }
        // Use the ratio directly as utility, rather than passing it through concave_utility. This
        // is because the likelihood a query will complete is a pretty straightforward conversion to
        // utility. Eg: If we send 3 queries to each indexer A and B, and A returns 1 success, and B
        // returns 3 successes - for some fixed value of a query the utility is number of returned
        // queries * value of query.
        let ratio = self.successful_queries / total_queries;
        let penalty = ((self.penalties / total_queries) + 1.0).recip();
        ratio * penalty
    }

    fn shift(&mut self, next: &Self, fraction: f64) {
        self.successful_queries *= fraction;
        self.successful_queries += next.successful_queries;
        self.failed_queries *= fraction;
        self.failed_queries += next.failed_queries;
        self.penalties *= fraction;
        self.penalties += next.penalties;
    }

    fn clear(&mut self) {
        self.successful_queries = 0.0;
        self.failed_queries = 0.0;
        self.penalties = 0.0;
    }

    fn count(&self) -> f64 {
        self.successful_queries + self.failed_queries
    }
}

impl Reputation {
    pub fn add_successful_query(&mut self) {
        self.successful_queries += 1.0;
    }

    pub fn add_failed_query(&mut self) {
        self.failed_queries += 1.0;
    }

    pub fn penalize(&mut self, weight: u8) {
        // Scale weight to a shift amount in range [0, 63].
        let shamt = (weight / 4).max(1) - 1;
        let weight = 1u64 << shamt;
        self.penalties += weight as f64;
    }
}
