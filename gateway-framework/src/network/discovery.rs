use alloy_primitives::BlockNumber;
use cost_model::CostModel;
use eventuals::Ptr;

#[derive(Clone)]
pub struct Status {
    pub block: BlockNumber,
    pub min_block: Option<BlockNumber>,
    pub cost_model: Option<Ptr<CostModel>>,
    pub legacy_scalar: bool,
    pub sql_enabled: bool,
}
