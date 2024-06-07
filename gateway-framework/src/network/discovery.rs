use alloy_primitives::BlockNumber;
use cost_model::CostModel;
use eventuals::Ptr;

use crate::gateway::http::gateway::IndexingStatus;

#[derive(Clone)]
pub struct Status {
    pub block: BlockNumber,
    pub min_block: Option<BlockNumber>,
    pub cost_model: Option<Ptr<CostModel>>,
    pub legacy_scalar: bool,
}

impl IndexingStatus for Status {
    fn block(&self) -> BlockNumber {
        self.block
    }

    fn min_block(&self) -> Option<BlockNumber> {
        self.min_block
    }

    fn legacy_scalar(&self) -> bool {
        self.legacy_scalar
    }
}
