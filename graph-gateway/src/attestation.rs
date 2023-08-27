use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_sol_types::{Eip712Domain, SolStruct as _};
use ethers::types::{RecoveryMessage, Signature};
use prost::Message;
use serde::{Deserialize, Serialize};
use toolshed::concat_bytes;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Attestation {
    #[serde(rename = "requestCID")]
    pub request_cid: B256,
    #[serde(rename = "responseCID")]
    pub response_cid: B256,
    #[serde(rename = "subgraphDeploymentID")]
    pub deployment: B256,
    pub v: u8,
    pub r: B256,
    pub s: B256,
}

pub struct Verifier {
    domain: Eip712Domain,
}

impl Verifier {
    pub fn new(chain_id: U256, dispute_manager: Address) -> Self {
        let salt: B256 = "0xa070ffb1cd7409649bf77822cce74495468e06dbfaef09556838bf188679b9c2"
            .parse()
            .unwrap();
        let domain = Eip712Domain {
            name: Some("Graph Protocol".into()),
            version: Some("0".into()),
            chain_id: Some(chain_id),
            verifying_contract: Some(dispute_manager),
            salt: Some(salt),
        };
        Self { domain }
    }

    pub fn verify(
        &self,
        attestation: &Attestation,
        indexer: &Address,
        request: &str,
        response: &str,
    ) -> Result<(), &'static str> {
        if attestation.request_cid != keccak256(request) {
            return Err("invalid request CID");
        }
        if attestation.response_cid != keccak256(response) {
            return Err("invalid response CID");
        }
        alloy_sol_types::sol! {
            struct AttestationMsg {
                bytes32 requestCID;
                bytes32 responseCID;
                bytes32 subgraphDeploymentID;
            }
        }
        let msg = AttestationMsg {
            requestCID: attestation.request_cid.0,
            responseCID: attestation.response_cid.0,
            subgraphDeploymentID: attestation.deployment.0,
        };
        let signing_hash: B256 = msg.eip712_signing_hash(&self.domain);
        let signature = Signature {
            r: attestation.r.0.into(),
            s: attestation.s.0.into(),
            v: attestation.v.into(),
        };
        let signer = signature
            .recover(RecoveryMessage::Hash(signing_hash.0.into()))
            .map_err(|_| "failed to recover signer")?;
        if signer.0 != indexer.0 {
            return Err("recovered signer is not expected indexer");
        }
        Ok(())
    }
}

impl Attestation {
    pub fn serialize_protobuf(
        &self,
        indexer: Address,
        request: String,
        response: String,
    ) -> Vec<u8> {
        AttestationProtobuf {
            request,
            response,
            indexer: indexer.0 .0.into(),
            subgraph_deployment: self.deployment.0.into(),
            request_cid: self.request_cid.0.into(),
            response_cid: self.response_cid.0.into(),
            signature: concat_bytes!(65, [&[self.v], &self.r.0, &self.s.0]).into(),
        }
        .encode_to_vec()
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct AttestationProtobuf {
    #[prost(string, tag = "1")]
    request: String,
    #[prost(string, tag = "2")]
    response: String,
    /// 20 bytes
    #[prost(bytes, tag = "3")]
    indexer: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "4")]
    subgraph_deployment: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "5")]
    request_cid: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "6")]
    response_cid: Vec<u8>,
    /// 65 bytes, ECDSA signature (v, r, s)
    #[prost(bytes, tag = "7")]
    signature: Vec<u8>,
}
