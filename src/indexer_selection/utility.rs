use std::f64::consts::E;

/// Concave Utility Function
/// u(x) = 1 - e ^ (-ax)
/// 0 <= a < INF
/// a != NaN
/// 0 <= x < INF
/// x != NaN
/// Returns: 0.0 <= u < 1.0
pub fn concave_utility(x: f64, a: f64) -> f64 {
    1.0 - E.powf(-1.0 * a * x)
}

#[derive(Debug)]
pub struct UtilityAggregator {
    mult: f64,
}

#[derive(Clone, Debug)]
pub struct SelectionFactor {
    pub weight: f64,
    pub utility: f64,
}

impl SelectionFactor {
    pub const fn zero() -> Self {
        Self {
            weight: 0.0,
            utility: 0.0,
        }
    }
    pub const fn one(utility: f64) -> Self {
        Self {
            weight: 1.0,
            utility,
        }
    }
}

impl UtilityAggregator {
    pub fn new() -> Self {
        Self { mult: 1.0 }
    }

    pub fn add(&mut self, factor: SelectionFactor) {
        let SelectionFactor { weight, utility } = factor;

        // One outcome we want is that any utility approaching 0
        // should seriously disadvantage the result.
        // For example, being behind a million blocks should mean never
        // getting selected even if the other utilities are good.
        //
        // Another important thing is that raising any utility should always raise the outcome
        // when holding the others constant. This ensures strictly dominant utility sets
        // give strictly dominant results. This limits the ways in which we can combine utilities.
        //
        // This uses the weighted product model.
        // See: https://en.wikipedia.org/wiki/Weighted_product_model
        // and https://towardsdatascience.com/free-yourself-from-indecision-with-weighted-product-models-48ae6fd5bf3
        // which satisfies the above.
        //
        // This used to use the geometric mean instead, but this favored bad indexers.
        // The first problem with the geometric mean is
        // that utilities near 0 do not disadvantage the result enough.
        // For example, given 5 factors with 4 utilities at 1.0 and
        // the last variable, the total utility does not go below 0.01
        // until the last variable goes down to an abysmal 0.0000000001,
        // and the total is above 0.25 even with a weight as low as 0.001.
        // In practice this is very bad. An indexer with any low utility
        // should almost never be selected (see above example about being
        // a million blocks behind).
        // The second problem that arose is that as more utilities were added,
        // bad indexers seemed to be favored more and more. A weighted multiply
        // just gives better results in practice.
        // The one thing that had to change to allow a weighted multiply was just
        // that before budget was a factor of the other utilities and this doesn't
        // make sense if there is no scale for utilities (eg: adding another utility
        // to consider shouldn't make the budget go down). With geometric mean that
        // wasn't a problem, but now it would be so price utility has been refactored
        // accordingly and the results are much better overall.
        self.mult *= utility.powf(weight);
    }

    pub fn crunch(self) -> f64 {
        self.mult
    }
}

#[cfg(test)]
mod tests {
    //! None of these tests are prescriptive, they
    //! are just here to describe the behavior that currently
    //! is. If the algorithm changes, it is fine to change
    //! the results

    use super::*;
    fn test(expect: f64, utils: &[(f64, f64)]) {
        let mut agg = UtilityAggregator::new();
        for util in utils.iter() {
            agg.add(SelectionFactor {
                utility: util.0,
                weight: util.1,
            });
        }
        let util = agg.crunch();
        assert_eq!(expect, util);
    }
    #[test]
    fn any_0_is_very_bad() {
        // Any utility of 0 must result in a final value of 0.
        test(0.0, &[(1.0, 1.0), (1.0, 1.0), (1.0, 1.0), (0.0, 1.0)]);
    }

    #[test]
    fn one_very_poor() {
        // Nearly the same as above, but one dropped down to 0.2
        // This should drastically affect the result.
        test(
            0.0620107539156828,
            &[(0.2, 0.9), (0.5, 0.8), (0.5, 1.0), (0.9, 0.8), (1.0, 1.0)],
        );
    }

    #[test]
    fn poor_with_low_weight() {
        // Same as above, but this time with low weight for the poor factor.
        test(
            0.2247206668370446,
            &[(0.2, 0.1), (0.5, 0.8), (0.5, 1.0), (0.9, 0.8), (1.0, 1.0)],
        );
    }

    #[test]
    fn pretty_good() {
        test(
            0.5232600000000001,
            &[(0.9, 1.0), (0.95, 1.0), (0.8, 1.0), (0.9, 1.0), (0.85, 1.0)],
        );
    }
}
