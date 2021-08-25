use crate::{
    indexer_selection::{utility::SelectionFactor, BadIndexerReason, Context, SelectionError},
    prelude::*,
};
use cost_model::{CompileError, CostError, CostModel};
use eventuals::EventualExt;
use std::convert::TryFrom;
use tree_buf::{Decode, Encode};

#[derive(Clone, Debug, Decode, Encode, Eq, Hash, PartialEq)]
pub struct CostModelSource {
    pub model: String,
    pub globals: String,
}

pub struct PriceEfficiency {
    pub model_src: Eventual<CostModelSource>,
    model: Eventual<Result<Ptr<CostModel>, Ptr<CompileError>>>,
}

impl PriceEfficiency {
    pub fn new(model_src: Eventual<CostModelSource>) -> Self {
        let model = model_src.clone().map(|src| async {
            CostModel::compile(src.model, &src.globals)
                .map(Ptr::new)
                .map_err(Ptr::new)
        });
        Self { model_src, model }
    }

    pub async fn get_price(
        &self,
        context: &mut Context<'_>,
        weight: f64,
        max_budget: &GRT,
    ) -> Result<(USD, SelectionFactor), SelectionError> {
        let model = self
            .model
            .value_immediate()
            .and_then(Result::ok)
            .ok_or(BadIndexerReason::MissingCostModel)?;
        let cost = match model.cost_with_context(context) {
            Err(CostError::CostModelFail) | Err(CostError::QueryNotCosted) => {
                return Err(BadIndexerReason::QueryNotCosted)?;
            }
            Err(CostError::QueryNotSupported) | Err(CostError::QueryInvalid) => {
                return Err(SelectionError::BadInput)
            }
            Err(CostError::FailedToParseQuery) | Err(CostError::FailedToParseVariables) => {
                unreachable!("Model did not parse, but failed in doing so.")
            }
            Ok(cost) => cost,
        };
        let fee: GRT = cost.to_string().parse::<GRTWei>().unwrap().shift();

        // Anything overpriced is refused.
        if fee > *max_budget {
            return Err(BadIndexerReason::FeeTooHigh.into());
        }

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

        // Solve the equation (1/(x+n))-n for n to intersect axis at desired places
        let scale = (-1.0 + 5.0f64.sqrt()) / 2.0;
        let one_wei: GRT = GRTWei::try_from(1u64).unwrap().shift();
        let scaled_fee = fee / max_budget.saturating_add(one_wei);
        let mut utility = (1.0 / (scaled_fee.as_f64() + scale)) - scale;
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::indexer_selection::test_utils::{assert_within, default_cost_model};
    use crate::prelude::test_utils::BASIC_QUERY;

    #[tokio::test]
    async fn price_efficiency() {
        let efficiency = PriceEfficiency::new(Eventual::from_value(default_cost_model(
            "0.01".parse().unwrap(),
        )));
        let mut context = Context::new(BASIC_QUERY, "").unwrap();
        // Expected values based on https://www.desmos.com/calculator/kxd4kpjxi5
        let tests = [(0.01, 0.0), (0.02, 0.2763), (0.1, 0.7746), (1.0, 0.9742)];
        for (budget, expected_utility) in tests {
            let (fee, utility) = efficiency
                .get_price(
                    &mut context,
                    0.5,
                    &budget.to_string().parse::<GRT>().unwrap(),
                )
                .await
                .unwrap();
            println!("fee: {}, {:?}", fee, utility);
            assert_eq!(fee, "0.01".parse::<GRT>().unwrap());
            assert_within(utility, expected_utility, 0.0001);
        }
    }
}
