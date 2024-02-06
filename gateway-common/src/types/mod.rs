use alloy_primitives::Address;
use thegraph::types::DeploymentId;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Indexing {
    pub indexer: Address,
    pub deployment: DeploymentId,
}
