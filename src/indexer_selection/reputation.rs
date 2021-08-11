use crate::indexer_selection::{
    utility::{concave_utility, SelectionFactor},
    SelectionError,
};
use tree_buf;

#[derive(Debug, Default, Clone, tree_buf::Decode, tree_buf::Encode)]
pub struct Reputation {
    successful_queries: f64,
    failed_queries: f64,
}

impl Reputation {
    pub fn add_successful_query(&mut self) {
        self.successful_queries += 1.0;
    }

    pub fn add_failed_query(&mut self) {
        self.failed_queries += 1.0;
    }

    pub fn expected_utility(&self) -> Result<SelectionFactor, SelectionError> {
        // TODO: Other factors like how long the indexer has been in the network.
        // Because reliability (which is what is captured here now) is useful on it's
        // own, it may be useful to separate this into it's own utility rather than
        // figure out how to combine that with other "reputation" factors which are much
        // more subjective.

        // Give the indexer the benefit of the doubt by pretending they have at least
        // 1 successful query. This prevents a divide by 0, but also keeps this utility
        // from reaching 0 which would be a point of no return since it can't correct
        // without issuing new queries.
        let successful_queries = self.successful_queries.max(1.0);
        let total = successful_queries + self.failed_queries;
        let ratio = successful_queries as f64 / total as f64;

        // Using the ratio directly as utility, rather than passing it through
        // concave_utility. This is because the likelihood a query will complete
        // is a pretty straightforward conversion to utility. Eg: If we send
        // 3 queries to each indexer A and B, and A returns 1 success, and B
        // returns 3 successes - for some fixed value of a query the utility
        // is number of returned queries * value of query.
        //
        // The use of concave_utility in the weight is to assign a confidence
        Ok(SelectionFactor {
            utility: ratio,
            // This weight gives about 85% confidence after 10 samples
            // We would like more samples, but the query volume per indexer/deployment
            // pair is so low that it otherwise takes a very long time to converge.
            weight: concave_utility(total as f64, 0.19),
        })
    }

    pub fn decay(&mut self, retain: f64) {
        self.failed_queries *= retain;
        self.successful_queries *= retain;
    }
}
