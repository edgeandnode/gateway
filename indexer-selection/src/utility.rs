use std::f64::consts::E;

#[derive(Clone, Copy, Debug)]
pub struct ConcaveUtilityParameters {
    pub a: f64,
    pub weight: f64,
}

impl ConcaveUtilityParameters {
    pub fn one(a: f64) -> Self {
        Self { a, weight: 1.0 }
    }

    /// Concave Utility Function
    /// u(x) = 1 - e ^ (-ax)
    /// 0 <= a < INF
    /// a != NaN
    /// 0 <= x < INF
    /// x != NaN
    /// Returns: 0.0 <= u < 1.0
    pub fn concave_utility(&self, x: f64) -> UtilityFactor {
        UtilityFactor {
            utility: 1.0 - E.powf(-self.a * x),
            weight: self.weight,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct UtilityFactor {
    pub utility: f64,
    pub weight: f64,
}

impl UtilityFactor {
    /// Just a utility value and a weight of 1.0
    pub const fn one(utility: f64) -> Self {
        Self {
            utility,
            weight: 1.0,
        }
    }

    pub fn mul_weight(mut self, factor: f64) -> Self {
        self.weight *= factor;
        self
    }
}

/// One outcome we want is that any utility approaching 0 should seriously disadvantage the result.
/// For example, being behind a million blocks should mean never getting selected even if the other
/// utilities are good.
///
/// Another important thing is that raising any utility should always raise the outcome when holding
/// the others constant. This ensures strictly dominant utility sets give strictly dominant results.
/// This limits the ways in which we can combine utilities.
///
/// This uses the weighted product model.
/// See: https://en.wikipedia.org/wiki/Weighted_product_model and
/// https://towardsdatascience.com/free-yourself-from-indecision-with-weighted-product-models-48ae6fd5bf3
/// which satisfies the above.
///
/// This used to use the geometric mean instead, but this favored bad indexers. The first problem
/// with the geometric mean is that utilities near 0 do not disadvantage the result enough. For
/// example, given 5 factors with 4 utilities at 1.0 and the last variable, the total utility does
/// not go below 0.01 until the last variable goes down to an abysmal 0.0000000001, and the total is
/// above 0.25 even with a weight as low as 0.001. In practice this is very bad. An indexer with any
/// low utility should almost never be selected (see above example about being a million blocks
/// behind). The second problem that arose is that as more utilities were added, bad indexers seemed
/// to be favored more and more.
/// A weighted multiply just gives better results in practice. The one thing that had to change to
/// allow a weighted multiply was just that before budget was a factor of the other utilities and
/// this doesn't make sense if there is no scale for utilities (eg: adding another utility to
/// consider shouldn't make the budget go down). With geometric mean that wasn't a problem, but now
/// it would be so fee utility has been refactored accordingly and the results are much better
/// overall.
pub fn weighted_product_model(factors: impl IntoIterator<Item = UtilityFactor>) -> f64 {
    factors
        .into_iter()
        .fold(1.0, |aggregate, UtilityFactor { utility, weight }| {
            debug_assert!(utility >= 0.0);
            aggregate * utility.powf(weight)
        })
}
