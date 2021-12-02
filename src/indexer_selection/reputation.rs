use crate::indexer_selection::{decay::Decay, SelectionError};

// TODO: Other factors like how long the indexer has been in the network.
// Because reliability (which is what is captured here now) is useful on it's own, it may be useful
// to separate this into it's own utility rather than figure out how to combine that with other
// "reputation" factors which are much more subjective.

#[derive(Debug, Default, Clone)]
pub struct Reputation {
    successful_queries: f64,
    failed_queries: f64,
}

impl Decay<Reputation> for Reputation {
    fn expected_utility(&self) -> Result<f64, SelectionError> {
        // Give the indexer the benefit of the doubt by pretending they have at least 1 successful
        // query. This prevents a divide by 0, but also keeps this utility from reaching 0 which
        // would be a point of no return since it can't correct without issuing new queries.
        let successful_queries = self.successful_queries.max(1.0);
        let total = successful_queries + self.failed_queries;
        let ratio = successful_queries as f64 / total as f64;

        // Use the ratio directly as utility, rather than passing it through concave_utility. This
        // is because the likelihood a query will complete is a pretty straightforward conversion to
        // utility. Eg: If we send 3 queries to each indexer A and B, and A returns 1 success, and B
        // returns 3 successes - for some fixed value of a query the utility is number of returned
        // queries * value of query.
        Ok(ratio)
    }

    fn shift(&mut self, next: &Self, fraction: f64) {
        self.successful_queries *= fraction;
        self.successful_queries += next.successful_queries;
        self.failed_queries *= fraction;
        self.failed_queries += next.failed_queries;
    }
}

impl Reputation {
    pub fn add_successful_query(&mut self) {
        self.successful_queries += 1.0;
    }

    pub fn add_failed_query(&mut self) {
        self.failed_queries += 1.0;
    }
}
