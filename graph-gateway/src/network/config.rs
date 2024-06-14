use semver::Version;

/// The minimum version requirements for the indexer.
#[derive(Debug, Clone)]
pub struct VersionRequirements {
    /// The minimum indexer  version.
    pub min_indexer_service_version: Version,
    /// The minimum graph node version.
    pub min_graph_node_version: Version,
}

impl Default for VersionRequirements {
    fn default() -> Self {
        Self {
            min_indexer_service_version: Version::new(0, 0, 0),
            min_graph_node_version: Version::new(0, 0, 0),
        }
    }
}
