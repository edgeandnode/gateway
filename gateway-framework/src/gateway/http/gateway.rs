use alloy_primitives::BlockNumber;

pub trait IndexingStatus: Send + Sync + 'static {
    fn block(&self) -> BlockNumber;
    fn min_block(&self) -> Option<BlockNumber>;
    fn legacy_scalar(&self) -> bool;
}
