use crate::{
    decay::{Decay, DecayUtility},
    impl_struct_decay,
    score::Merge,
};

// TODO: Other factors like how long the indexer has been in the network.
// Because reliability (which is what is captured here now) is useful on it's own, it may be useful
// to separate this into it's own utility rather than figure out how to combine that with other
// "reputation" factors which are much more subjective.

#[derive(Debug, Default, Clone, Copy)]
pub struct Reputation {
    successful_queries: f64,
    failed_queries: f64,
    penalties: f64,
}

impl Merge for Reputation {
    fn merge(&mut self, other: &Self) {
        self.successful_queries += other.successful_queries;
        self.failed_queries += other.failed_queries;
        self.penalties += other.penalties;
    }
}

impl_struct_decay!(Reputation {
    successful_queries,
    failed_queries,
    penalties
});

impl DecayUtility for Reputation {
    fn expected_utility(&self, _u_a: f64) -> f64 {
        // Need to add a free success so that no buckets have a utility of 0.
        // Even with a non-1 weight a utility of 0 ends up bringing the result
        // to 0 which we can't afford.
        let successful_queries = self.successful_queries + 0.5;
        let total_queries = successful_queries + self.failed_queries;

        // Use the ratio directly as utility, rather than passing it through concave_utility. This
        // is because the likelihood a query will complete is a pretty straightforward conversion to
        // utility. Eg: If we send 3 queries to each indexer A and B, and A returns 1 success, and B
        // returns 3 successes - for some fixed value of a query the utility is number of returned
        // queries * value of query.
        let mut ratio = successful_queries / total_queries;

        // We want a non-linear relationship between success rate and utility.
        // In a world which looks at count of nines, it's not acceptable to send
        // a 50% success indexer 50% queries.
        ratio *= ratio;

        let penalty = ((self.penalties / total_queries) + 1.0).recip();
        ratio * penalty
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
        self.penalties += 1.1_f64.powf(weight as f64);
    }
}
