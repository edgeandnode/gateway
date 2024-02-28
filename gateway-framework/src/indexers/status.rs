use alloy_primitives::BlockHash;
use serde::{Deserialize, Deserializer};
use std::borrow::Cow;
use thegraph::types::DeploymentId;

fn deserialize_bad_hex<'de, D>(deserializer: D) -> Result<BlockHash, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Cow::<str>::deserialize(deserializer)?;
    if s == "0x0" {
        return Ok(BlockHash::ZERO);
    }
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize)]
pub struct BlockInfo {
    pub number: String,
    #[serde(deserialize_with = "deserialize_bad_hex")]
    pub hash: BlockHash,
}

pub struct ChainIndexingStatus {
    pub chain: String,
    pub latest_block: BlockInfo,
    pub earliest_block: BlockInfo,
}

pub struct IndexingStatus {
    pub deployment: DeploymentId,
    pub chains: Vec<ChainIndexingStatus>,
}
