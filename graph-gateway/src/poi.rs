use alloy_primitives::{BlockNumber, B256};
use thegraph::types::DeploymentId;

pub type ProofOfIndexing = B256;

#[derive(Debug, Clone, Eq, PartialEq, Hash, serde::Deserialize)]
pub struct ProofOfIndexingInfo {
    /// Proof of indexing (POI).
    pub proof_of_indexing: ProofOfIndexing,
    /// POI deployment ID (the IPFS Hash in the Graph Network Subgraph).
    pub deployment_id: DeploymentId,
    /// POI block number.
    pub block_number: BlockNumber,
}

impl ProofOfIndexingInfo {
    /// Get the POI bytes.
    pub fn poi(&self) -> ProofOfIndexing {
        self.proof_of_indexing
    }

    /// Get the POI metadata.
    pub fn meta(&self) -> (DeploymentId, BlockNumber) {
        (self.deployment_id, self.block_number)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use serde_json::json;

    use super::*;

    #[test]
    fn deserialize_proof_of_indexing_info_config() {
        //// Given
        let poi: ProofOfIndexing =
            "0xba8a057796a81e013789789996551bb5b2920fb9947334db956992f7098bd287"
                .parse()
                .expect("Invalid POI");
        let deployment_id = "QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH"
            .parse()
            .expect("Invalid deployment ID/Ipfs hash");
        let block_number = 123;

        // Manually serialize into a JSON string.
        let json = json!(
            {
                "proof_of_indexing": poi,
                "deployment_id": deployment_id,
                "block_number": block_number,
            }
        )
        .to_string();

        //// When
        let info = serde_json::from_str::<ProofOfIndexingInfo>(&json);

        //// Then
        assert_matches!(info, Ok(poi_info) => {
                assert_eq!(poi_info.poi(), poi);
                assert_eq!(poi_info.meta(), (deployment_id, block_number));
        });
    }
}
