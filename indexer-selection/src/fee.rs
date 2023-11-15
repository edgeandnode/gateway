use std::convert::TryFrom;

use alloy_primitives::U256;
use cost_model::{CostError, CostModel};
use eventuals::Ptr;

use prelude::{UDecimal18, GRT};

use crate::{utility::UtilityFactor, Context, IndexerError, InputError, SelectionError};

// TODO: We need to rethink both of the following functions.
// - We have been treating fees as having relatively low importance to prioritize network health
//   over cost to dApp developers. The utility curve incentivises indexers to charge fees above 50%
//   of the query budget. With multi-selection, this prevents more than one indexer being selected,
//   driving down QoS for the client query.
// - The simplifying assumption that queries are proportional to utility was never true. But it's
//   especially untrue with multi-selection, since indexers with low fees & high utilities in a
//   single factor may be selected far more often then they were previously.

/// (5_f64.sqrt() - 1.0) / 2.0
const S: f64 = 0.6180339887498949;

// 7a3da342-c049-4ab0-8058-91880491b442
const WEIGHT: f64 = 0.5;

/// Constraints for the utility function `u(fee)`:
///   - u(0) = 1, u(budget + 1 GRTWei) = 0, and u is continuous within this input range. We want to
///     avoid a discontinuity at the budget where a 1 GRTWei difference in the fee suddenly sends
///     the utility to 0.
///   - monotonically decreasing with a range of [0,1] (the expected range of the utility combining
///     function)
///
/// Another consideration is what shape we want the curve to be. At first, is seems a straight line
/// would make sense - this would express that the utility of money is linear (eg: 2x money is 2x
/// the utility) and we are measuring the amount saved. (eg: if you pay 25% of budget you would
/// retain 75% money and if you pay no fee you would retain 100% money and the value of the money
/// is linear. This works out. But.. there's another way to think about it. Gateways buy queries in
/// bulk. If we flip the question to "how many queries can I buy with a given amount of money" we
/// get a different answer - 1/x. Eg: We can buy 1 query at 100%, or 4 queries at 25%, so it is 4x
/// the utility to buy 4 queries. 1/x though doesn't intersect (0,1) and (1,0) though. So, we just
/// take 1/x and shift the curve down and to the left so that it intersects those points. This
/// answer is a compromise between linear and 1/x.
///
/// One annoying thing about treating this as a separate utility, rather than have some notion of
/// being "willing to pay more for more service" is that intuitively it seems that fee being used
/// for service isn't "wasted". Eg: I should be willing to pay more for higher service, and it's the
/// difference between the expected fee and actual fee for the product that is savings or losses
/// and this doesn't take that into account at all. But my hunch is that the utility combining
/// function captures this and it's ok to have it be separate like this.
///
/// I (Zac) did some math too see what the optimal fee (as a proportion of budget) should be for
/// an Indexer to maximize their own revenue given the current algorithm.
/// Toy is here: https://www.desmos.com/calculator/hsex58vkc0
/// For the rendered curve - x is fee, y is revenue. The maxima is the point of interest, where
/// the given fee maximizes revenue.
/// - The top value n=... is the scale value above.
/// - The u(utility) equation is the utility function for this indexer
/// - The m(my) slider is the Indexer's own utility for all other utilities that are not fee
/// - The o(other) slider is the average utility of other Indexers in the pool (after accounting for
///   their fee)
/// - The c(count) slider is the number of other Indexers in the pool.
///
/// This verifies some properties that we want to hold.
/// - An Indexer with better service (as compared to the total pool) is incentivized to charge a
///   higher fee than an Indexer with a lesser service in the same pool, and will increase overall
///   fees.
///   This can be verified by dragging m to the right and seeing the curve shift up and to the right.
/// - Even as the number of Indexers of a subgraph increases to 6, the optimal fee for an
///   average Indexer is 63% of the maximum budget (with higher optimal fees as the number
///   of Indexers decreases). This can be verified by setting o and m to equal values and
///   increasing c(count) to 5 (on second thought that's wrong... because m doesn't include fee - so
///   o would have to be lower than m - meaning the optimal fee is a bit higher).
/// - This ensures there is no race to the bottom.
/// - It seems that assuming mostly rational Indexers and a medium sized pool,
///   the Consumer may expect to pay ~55-75% of the maximum budget.

// https://www.desmos.com/calculator/wnffyb9edh
// 7a3da342-c049-4ab0-8058-91880491b442
pub fn fee_utility(fee: &GRT, budget: &GRT) -> UtilityFactor {
    // Any fee over budget has zero utility.
    if fee.0 > budget.0 {
        return UtilityFactor::one(0.0);
    }
    let one_wei = UDecimal18::from_raw_u256(U256::from(1));
    let scaled_fee: f64 = (fee.0 / budget.0.saturating_add(one_wei)).into();
    println!("{} {}, {scaled_fee}", fee.0, budget.0);
    let mut utility = (scaled_fee + S).recip() - S;
    // Set minimum utility, since small negative utility can result from loss of precision when the
    // fee approaches the budget.
    utility = utility.max(1e-18);
    UtilityFactor {
        utility,
        weight: WEIGHT,
    }
}

/// Indexers set their fees using cost models. However, indexers currently take a "lazy" approach to
/// setting cost models (e.g. `default => 0.0000001 (GRT)`). So we do some work for them by
/// calculating a lower bound for the revenue maximizing fee, while still giving them an opportunity
/// to benefit from optimizing their cost models.
///
/// We assume queries are proportional to utility. This is wrong, but it would simplify our revenue
/// function to `fee * utility`. Solving for the correct revenue maximizing value is complex and
/// recursive (since the revenue maximizing fee depends on the utility of all other indexers which
/// itself depends on their revenue maximizing fee... ad infinitum).
fn min_optimal_fee(budget: &GRT) -> GRT {
    let w = WEIGHT;
    let mut min_rate = (4.0 * S.powi(2) * w + w.powi(2) - 2.0 * w + 1.0).sqrt();
    min_rate = (min_rate - 2.0 * S.powi(2) - w + 1.0) / (2.0 * S);
    GRT(budget.0 * UDecimal18::try_from(min_rate).unwrap())
}

pub fn indexer_fee(
    cost_model: &Option<Ptr<CostModel>>,
    context: &mut Context<'_>,
    budget: &GRT,
    max_indexers: u8,
) -> Result<GRT, SelectionError> {
    let mut fee = match cost_model
        .as_ref()
        .map(|model| model.cost_with_context(context))
    {
        None => GRT(UDecimal18::from(0)),
        Some(Ok(fee)) => {
            let fee = U256::try_from_be_slice(&fee.to_bytes_be()).unwrap_or(U256::MAX);
            GRT(UDecimal18::from_raw_u256(fee))
        }
        Some(Err(CostError::CostModelFail | CostError::QueryNotCosted)) => {
            return Err(IndexerError::QueryNotCosted.into());
        }
        Some(Err(
            CostError::QueryNotSupported
            | CostError::QueryInvalid
            | CostError::FailedToParseQuery
            | CostError::FailedToParseVariables,
        )) => {
            return Err(InputError::MalformedQuery.into());
        }
    };

    // Any fee over budget is refused.
    if fee.0 > budget.0 {
        return Err(IndexerError::FeeTooHigh.into());
    }

    let budget = GRT(budget.0 / UDecimal18::from(max_indexers as u128));
    let min_optimal_fee = min_optimal_fee(&budget);
    // If their fee is less than the min optimal, lerp between them so that
    // indexers are rewarded for being closer.
    if fee.0 < min_optimal_fee.0 {
        fee = GRT((min_optimal_fee.0 + fee.0) * UDecimal18::try_from(0.75).unwrap());
    }

    Ok(fee)
}

#[cfg(test)]
mod test {
    use prelude::test_utils::BASIC_QUERY;

    use crate::test_utils::{assert_within, default_cost_model};

    use super::*;

    #[test]
    fn test() {
        let cost_model = Some(Ptr::new(default_cost_model(GRT(UDecimal18::try_from(
            0.01,
        )
        .unwrap()))));
        let mut context = Context::new(BASIC_QUERY, "").unwrap();
        // Expected values based on https://www.desmos.com/calculator/kxd4kpjxi5
        let tests = [(0.01, 0.0), (0.02, 0.27304), (0.1, 0.50615), (1.0, 0.55769)];
        for (budget, expected_utility) in tests {
            let budget = GRT(UDecimal18::try_from(budget).unwrap());
            let fee = indexer_fee(&cost_model, &mut context, &budget, 1).unwrap();
            let utility = fee_utility(&fee, &budget);
            println!("{fee:?} / {budget:?}, {expected_utility}, {utility:?}");
            let utility = utility.utility.powf(utility.weight);
            assert!(fee.0 >= UDecimal18::try_from(0.01).unwrap());
            assert_within(utility, expected_utility, 0.0001);
        }
    }
}
