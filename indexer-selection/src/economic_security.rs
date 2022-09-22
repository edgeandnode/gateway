use crate::utility::{concave_utility, SelectionFactor, UtilityParameters};
use prelude::*;

pub struct EconomicSecurity {
    pub slashable: USD,
    pub utility: SelectionFactor,
}

#[derive(Default)]
pub struct NetworkParameters {
    pub slashing_percentage: Option<PPM>,
    pub usd_to_grt_conversion: Option<GRT>,
}

impl NetworkParameters {
    pub fn usd_to_grt(&self, usd: USD) -> Option<GRT> {
        let conversion_rate = self.usd_to_grt_conversion?;
        Some(usd * conversion_rate)
    }

    pub fn grt_to_usd(&self, grt: GRT) -> Option<USD> {
        let conversion_rate = self.usd_to_grt_conversion?;
        Some(grt / conversion_rate)
    }

    pub fn economic_security_utility(
        &self,
        indexer_stake: GRT,
        utility_parameters: UtilityParameters,
    ) -> Option<EconomicSecurity> {
        let slashing_percentage = self.slashing_percentage?;
        let slashable_grt = indexer_stake * slashing_percentage.change_precision();
        let slashable = self.grt_to_usd(slashable_grt)?;
        let utility = concave_utility(slashable.as_f64(), utility_parameters.a);
        Some(EconomicSecurity {
            slashable,
            utility: SelectionFactor {
                utility,
                weight: utility_parameters.weight,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn two_usd_to_grt() {
        let params = NetworkParameters {
            usd_to_grt_conversion: "0.511732966311998143".parse().ok(),
            slashing_percentage: 0u64.try_into().ok(),
        };
        // Check conversion of 2 USD to GRT
        assert_eq!(
            params.usd_to_grt(2u64.try_into().unwrap()),
            Some("1.023465932623996286".parse().unwrap())
        );
    }

    #[test]
    fn two_grt_to_usd() {
        let params = NetworkParameters {
            usd_to_grt_conversion: "0.511732966311998143".parse().ok(),
            slashing_percentage: 0u64.try_into().ok(),
        };
        // Check conversion of 2 GRT to USD
        assert_eq!(
            params.grt_to_usd(2u64.try_into().unwrap()),
            Some("3.908288368470326937".parse().unwrap())
        );
    }

    #[test]
    fn trillion_usd_to_grt() {
        let mut params = NetworkParameters {
            usd_to_grt_conversion: None,
            slashing_percentage: 0u64.try_into().ok(),
        };
        let trillion: USD = 10u64.pow(12).try_into().unwrap();
        // Assert None is returned if eventual has no value.
        assert_eq!(params.usd_to_grt(trillion), None);
        // Set conversion rate
        params.usd_to_grt_conversion = "0.511732966311998143".parse().ok();
        // Check conversion of 1 trillion USD to GRT
        assert_eq!(
            params.usd_to_grt(trillion),
            Some("511732966311.998143".parse().unwrap())
        );
    }

    #[test]
    fn trillion_grt_to_usd() {
        let mut params = NetworkParameters {
            usd_to_grt_conversion: None,
            slashing_percentage: 0u64.try_into().ok(),
        };
        let trillion: GRT = 10u64.pow(12).try_into().unwrap();
        // Assert None is returned if eventual has no value.
        assert_eq!(params.grt_to_usd(trillion), None);
        // Set conversion rate
        params.usd_to_grt_conversion = "0.511732966311998143".parse().ok();
        // Check conversion of 1 trillion GRT to USD
        assert_eq!(
            params.grt_to_usd(trillion),
            Some("1954144184235.163468761907206198".parse().unwrap())
        );
    }

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
        usd_to_grt: u64,
        slashing_percentage: f64,
        stake: u64,
        u_a: f64,
        expected_slashable: u64,
        expected_utility: f64,
    ) {
        let params = NetworkParameters {
            usd_to_grt_conversion: usd_to_grt.try_into().ok(),
            slashing_percentage: slashing_percentage.to_string().parse().ok(),
        };
        let security = params
            .economic_security_utility(stake.try_into().unwrap(), UtilityParameters::one(u_a))
            .unwrap();
        assert_eq!(security.slashable, expected_slashable.try_into().unwrap());
        assert_within(security.utility.utility, expected_utility, 0.01);
    }
}
