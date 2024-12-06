#[derive(Clone, Debug, thiserror::Error)]
pub enum SubgraphError {
    /// No allocations were found for the subgraph.
    #[error("no allocations")]
    NoAllocations,

    /// All subgraph versions were marked as invalid.
    #[error("no valid versions")]
    NoValidVersions,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum DeploymentError {
    /// No allocations were found for the subgraph.
    #[error("no allocations")]
    NoAllocations,
}
