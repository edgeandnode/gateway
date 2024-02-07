use indexer_selection::NotNan;

/// User query settings typically associated with an auth token.
#[derive(Clone, Debug, Default)]
pub struct QuerySettings {
    pub budget_usd: Option<NotNan<f64>>,
}
