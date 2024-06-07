use ordered_float::NotNan;
use tokio::sync::watch;

use crate::{auth::AuthSettings, budgets::Budgeter, errors::Error};

pub struct Budget {
    pub budget: u128,
    pub one_grt: NotNan<f64>,
    pub grt_per_usd: NotNan<f64>,
}

impl Budget {
    pub fn calculate(
        grt_per_usd: &watch::Receiver<NotNan<f64>>,
        budgeter: &Budgeter,
        settings: &AuthSettings,
    ) -> Result<Self, Error> {
        let grt_per_usd = *grt_per_usd.borrow();
        let one_grt = NotNan::new(1e18).unwrap();
        let mut budget = *(budgeter.query_fees_target.0 * grt_per_usd * one_grt) as u128;
        if let Some(user_budget_usd) = settings.budget_usd {
            // Security: Consumers can and will set their budget to unreasonably high values.
            // This `.min` prevents the budget from being set far beyond what it would be
            // automatically. The reason this is important is that sometimes queries are
            // subsidized, and we would be at-risk to allow arbitrarily high values.
            let max_budget = budget * 10;
            budget = (*(user_budget_usd * grt_per_usd * one_grt) as u128).min(max_budget);
        }

        Ok(Budget {
            budget,
            one_grt,
            grt_per_usd,
        })
    }
}
