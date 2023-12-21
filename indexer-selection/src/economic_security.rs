use gateway_common::types::{UDecimal18, GRT, USD};

#[derive(Default)]
pub struct NetworkParameters {
    pub slashing_percentage: Option<UDecimal18>,
    pub grt_per_usd: Option<GRT>,
}

impl NetworkParameters {
    pub fn slashable_usd(&self, indexer_stake: GRT) -> Option<USD> {
        let slashing_percentage = self.slashing_percentage?;
        let slashable_grt = indexer_stake.0 * slashing_percentage;
        let slashable_usd = slashable_grt / self.grt_per_usd?.0;
        Some(USD(slashable_usd))
    }
}

#[cfg(test)]
mod tests {
    use gateway_common::utils::testing::assert_within;

    use crate::utility::ConcaveUtilityParameters;

    use super::*;

    #[test]
    fn high_stake() {
        // $1m dollars amounts to ~80% utility
        test_economic_security_utility(1_000, 0.5, 2_000_000_000, 0.00000161757, 1_000_000, 0.8);
    }

    #[test]
    fn low_stake() {
        // $100k amounts to 15% utility
        test_economic_security_utility(100, 0.05, 200_000_000, 0.00000161757, 100_000, 0.15);
    }

    #[test]
    fn low_a() {
        // Different u_a for a case where $10k is plenty of utility
        test_economic_security_utility(1, 1.0, 10_000, 0.00016, 10_000, 0.8);
    }

    #[test]
    fn testnet_a() {
        // For the testnet, expecting ~$400k slashable for these parameters.
        // Each Indexer gets $5m, and slashing percent is 10.
        test_economic_security_utility(1, 0.1, 4_000_000, 0.000006, 400_000, 0.91);
    }

    fn test_economic_security_utility(
        grt_per_usd: u128,
        slashing_percentage: f64,
        stake: u128,
        u_a: f64,
        expected_slashable: u128,
        expected_utility: f64,
    ) {
        let params = NetworkParameters {
            grt_per_usd: Some(GRT(UDecimal18::from(grt_per_usd))),
            slashing_percentage: UDecimal18::try_from(slashing_percentage).ok(),
        };
        let slashable = params.slashable_usd(GRT(UDecimal18::from(stake))).unwrap();
        let utility = ConcaveUtilityParameters::one(u_a).concave_utility(slashable.0.into());
        assert_eq!(slashable.0, UDecimal18::from(expected_slashable));
        assert_within(utility.utility, expected_utility, 0.01);
    }
}
