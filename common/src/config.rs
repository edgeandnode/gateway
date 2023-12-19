use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use toolshed::url::Url;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct Chain {
    /// The first name is used in logs, the others are aliases also supported in subgraph manifests.
    pub names: Vec<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub rpc: Url,
}
