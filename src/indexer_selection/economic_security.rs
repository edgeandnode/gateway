use crate::prelude::*;

pub struct EconomicSecurity {
    pub slashable_usd: f64,
    pub utility: SelectionFactor,
}

pub struct NetworkParameters {
    pub slashing_percentage: Eventual<PPM>,
    pub usd_to_grt_conversion: Eventual<USD>,
}

impl NetworkParameters {
    pub fn usd_to_grt(&self, usd: USD) -> Option<GRT> {
        let conversion_rate = self.usd_to_grt_conversion.value_immediate()?;
        Some(usd * conversion_rate)
    }

    pub fn grt_to_usd(&self, grt: GRT) -> Option<USD> {
        let conversion_rate = self.usd_to_grt_conversion.value_immediate()?;
        Some(grt / conversion_rate)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time;

    #[tokio::test]
    async fn two_usd_to_grt() {
        let (mut conversion_writer, conversion) = Eventual::new();
        let params = NetworkParameters {
            usd_to_grt_conversion: conversion,
            slashing_percentage: Eventual::from_value(0.into()),
        };
        // Assert None is returned if eventual has no value.
        assert_eq!(params.grt_to_usd(2.into()), None);
        // Set conversion rate
        conversion_writer.write("0.511732966311998143".parse().unwrap());
        time::sleep(time::Duration::from_millis(100)).await;
        // Check conversion of 2 USD to GRT
        assert_eq!(
            params.usd_to_grt(2.into()),
            Some("1.023465932623996286".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn two_grt_to_usd() {
        let (mut conversion_writer, conversion) = Eventual::new();
        let params = NetworkParameters {
            usd_to_grt_conversion: conversion,
            slashing_percentage: Eventual::from_value(0.into()),
        };
        // Assert None is returned if eventual has no value.
        assert_eq!(params.grt_to_usd(2.into()), None);
        // Set conversion rate
        conversion_writer.write("0.511732966311998143".parse().unwrap());
        time::sleep(time::Duration::from_millis(100)).await;
        // Check conversion of 2 GRT to USD
        assert_eq!(
            params.grt_to_usd(2.into()),
            Some("3.908288368470326937".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn trillion_usd_to_grt() {
        let (mut conversion_writer, conversion) = Eventual::new();
        let params = NetworkParameters {
            usd_to_grt_conversion: conversion,
            slashing_percentage: Eventual::from_value(0.into()),
        };
        let trillion: USD = 10u64.pow(12).into();
        // Assert None is returned if eventual has no value.
        assert_eq!(params.usd_to_grt(trillion), None);
        // Set conversion rate
        conversion_writer.write("0.511732966311998143".parse().unwrap());
        time::sleep(time::Duration::from_millis(100)).await;
        // Check conversion of 1 trillion USD to GRT
        assert_eq!(
            params.usd_to_grt(trillion),
            Some("511732966311.998143".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn trillion_grt_to_usd() {
        let (mut conversion_writer, conversion) = Eventual::new();
        let params = NetworkParameters {
            usd_to_grt_conversion: conversion,
            slashing_percentage: Eventual::from_value(0.into()),
        };
        let trillion: GRT = 10u64.pow(12).into();
        // Assert None is returned if eventual has no value.
        assert_eq!(params.grt_to_usd(trillion), None);
        // Set conversion rate
        conversion_writer.write("0.511732966311998143".parse().unwrap());
        time::sleep(time::Duration::from_millis(100)).await;
        // Check conversion of 1 trillion GRT to USD
        assert_eq!(
            params.grt_to_usd(trillion),
            Some("1954144184235.163468761907206198".parse().unwrap())
        );
    }
}
