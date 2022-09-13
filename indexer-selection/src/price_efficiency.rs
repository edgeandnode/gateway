use crate::{utility::SelectionFactor, BadIndexerReason, Context, SelectionError};
use cost_model::{CostError, CostModel};
use prelude::*;
use std::convert::TryFrom;

pub fn get_price(
    cost_model: &Option<Ptr<CostModel>>,
    context: &mut Context<'_>,
    weight: f64,
    max_budget: &GRT,
) -> Result<(USD, SelectionFactor), SelectionError> {
    let mut fee = match cost_model
        .as_ref()
        .map(|model| model.cost_with_context(context))
    {
        None => GRT::zero(),
        Some(Ok(fee)) => fee.to_string().parse::<GRTWei>().unwrap().shift(),
        Some(Err(CostError::CostModelFail | CostError::QueryNotCosted)) => {
            return Err(BadIndexerReason::QueryNotCosted)?;
        }
        Some(Err(CostError::QueryNotSupported | CostError::QueryInvalid)) => {
            return Err(SelectionError::MalformedQuery);
        }
        Some(Err(CostError::FailedToParseQuery | CostError::FailedToParseVariables)) => {
            unreachable!("Model did not parse, but failed in doing so.")
        }
    };

    // Anything overpriced is refused.
    if fee > *max_budget {
        return Err(BadIndexerReason::FeeTooHigh.into());
    }

    // Set the minimum fee to a value which is lower than the
    // revenue maximizing price. This is sort of a hack because
    // indexers can't be bothered to optimize their cost models.
    // We avoid query fees going too low, but also make it so that
    // indexers can maximize their revenue by raising their prices
    // to reward those who are putting more effort in than "default => 0.0000001;"
    //
    // This is somewhat hard, so we'll make some assumptions:
    // First assumption: queries are proportional to utility. This is wrong,
    // but it would simplify our revenue function to queries * utility
    // Given:
    //   s = (-1.0 + 5.0f64.sqrt()) / 2.0
    //   x = queries
    //   w = weight
    //   p = normalized price
    //   u = utility = ((1.0 / (p + s)) - s) ^ w
    // Then the derivative of x * u is:
    //   ((1 / (p + s) - s)^w - (w*p / (((p + s)^2) * ((1/(p+s)) - s)^(1.0-w))
    // And, given a w, we want to solve for p such that the derivative is 0.
    // This gives us the lower bound for the revenue maximizing price.
    //
    // Solving gives us the following:
    // https://www.desmos.com/calculator/of533qsdla
    //
    // I think this approximation is of a lower bound is reasonable
    // (better not to overestimate and hurt the indexer's revenue)
    // Solving for the correct revenue-maximizing value is complex and recursive (since the revenue
    // maximizing price depends on the utility of all other indexers which itself depends
    // on their revenue maximizing price... ad infinitum)
    //
    // Solve the equation (1/(x+n))-n for n to intersect axis at desired places
    let s = (-1.0 + 5.0f64.sqrt()) / 2.0;
    let w = weight;
    let min_rate = ((4.0 * s.powi(2) * w + w.powi(2) - 2.0 * w + 1.0).sqrt() - 2.0 * s.powi(2) - w
        + 1.0)
        / (2.0 * s);

    let min_rate = GRT::try_from(min_rate).unwrap();
    let min_optimal_fee = *max_budget * min_rate;
    // If their fee is less than the min optimal, lerp between them so that
    // indexers are rewarded for being closer.
    if fee < min_optimal_fee {
        fee = (min_optimal_fee + fee) * GRT::try_from(0.75).unwrap();
    }

    //

    // I went over a bunch of options and am not happy with any of them.
    // Also this is hard to summarize but I'll take a shot.
    // It may help before explaining what this code does to understand
    // some of the constraints.
    //
    // First, a price of 0 should have utility 1, and max_price + 1 should
    // have a utility of 0 and the function should be continuous between.
    // This prevents a discontinuity at max_price where all of a sudden with
    // 1 wei difference it goes from some utility like 0.5 to 0 because of
    // the BelowMinimumRequirements error. The range 0-1 keeps it in the
    // expected range of the utility combining function, which does not scale
    // factors but expects them in this range.
    //
    // Another consideration is what shape we want the curve to be. At first
    // I thought a straight line would make sense - this would express that
    // the utility of money is linear (eg: 2x money is 2x the utility) and
    // we are measuring the amount saved. (eg: if you pay 25% of max_budget
    // you would retain 75% money and if you pay no price you would retain 100% money and
    // the value of the money is linear. This works out.
    // But..
    // There's another way to think about it. The Gateway buys queries in bulk.
    // If we flip the question to "how many queries can I buy given a given amount
    // of money" we get a different answer - 1/x. Eg: We can buy 1 query at 100% price,
    // or 4 queries at 25% price, so it is 4x the utility to buy 4 queries.
    // 1/x though doesn't intersect (0,1) and (1,0) though. So, what I've done
    // is to just take 1/x and shift the curve down and to the left so that it
    // intersects those points. This answer is a compromise between linear and 1/x.
    //
    // One thing that bothers me about treating this as a separate utility, rather than
    // have some notion of being "willing to pay more for more service" is that intuitively
    // it seems that price being used for service isn't "wasted". Eg: I should be willing
    // to pay more for higher service, and it's the difference between the expected price
    // and actual price for the product that is savings or losses and this doesn't take that into
    // account at all.
    // But my hunch is that the utility combining function captures this and it's ok
    // to have it be separate like this.

    let one_wei: GRT = GRTWei::try_from(1u64).unwrap().shift();
    let scaled_fee = fee / max_budget.saturating_add(one_wei);
    let mut utility = (1.0 / (scaled_fee.as_f64() + s)) - s;
    // Set minimum utility, since small negative utility can result from loss of precision when the fee approaches the budget.
    utility = utility.max(1e-18);

    // Did some math too see what the optimal price (as a proportion of budget) should
    // be for an Indexer to maximize their own revenue given the current algorithm.
    // Toy is here: https://www.desmos.com/calculator/hsex58vkc0
    // For the rendered curve - x is price, y is revenue. The maxima is the point of interest, where
    // the given price maximizes revenue.
    // The top value n=... is the scale value above.
    // The u(utility) equation is the utility function for this Indexer
    // The m(my) slider is the Indexer's own utility for all other utilities that are not price
    // The o(other) slider is the average utility of other Indexers in the pool (after accounting for their price)
    // The c(count) slider is the number of other Indexers in the pool.
    //
    // This verifies some properties that we want to hold.
    // * An Indexer with better service (as compared to the total pool) is incentivized to
    //   charge a higher price than an Indexer with a lesser service in the same pool, and will increase
    //   overall fees.
    //   This can be verified by dragging m to the right and seeing the curve shift up and to the right.
    // * Even as the number of Indexers of a subgraph increases to 6, the optimal price for an
    //   average Indexer is 63% of the maximum budget (with higher optimal prices as the number
    //   of Indexers decreases). This can be verified by setting o and m to equal values and
    //   increasing c(count) to 5 (on second thought that's wrong... because m doesn't include price - so
    //   o would have to be lower than m - meaning the optimal price is a bit higher).
    // * This ensures there is no race to the bottom.
    // * It seems that assuming mostly rational Indexers and a medium sized pool,
    //   the Consumer may expect to pay ~55-75% of the maximum budget.

    // Treat price as having relatively low importance to
    // prioritize network health over cost to dApp developers.
    let savings_utility = SelectionFactor { weight, utility };
    Ok((fee, savings_utility))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::{assert_within, default_cost_model};
    use prelude::test_utils::BASIC_QUERY;

    #[tokio::test]
    async fn price_efficiency() {
        let cost_model = Some(Ptr::new(default_cost_model("0.01".parse().unwrap())));
        eventuals::idle().await;
        let mut context = Context::new(BASIC_QUERY, "").unwrap();
        // Expected values based on https://www.desmos.com/calculator/kxd4kpjxi5
        let tests = [(0.01, 0.0), (0.02, 0.27304), (0.1, 0.50615), (1.0, 0.55769)];
        for (budget, expected_utility) in tests {
            let (fee, utility) = get_price(
                &cost_model,
                &mut context,
                0.5,
                &budget.to_string().parse::<GRT>().unwrap(),
            )
            .unwrap();
            let utility = utility.utility.powf(utility.weight);
            println!("fee: {}, {:?}", fee, utility);
            assert!(fee >= "0.01".parse::<GRT>().unwrap());
            assert_within(utility, expected_utility, 0.0001);
        }
    }
}
