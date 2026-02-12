//! TAP Receipt Signing
//!
//! Creates and signs Timeline Aggregation Protocol (TAP) receipts for indexer payments.
//! Receipts are cryptographic commitments to pay indexers for query execution.
//!
//! # Receipt Versions
//!
//! | Version | Contract | Allocation Type | Serialization |
//! |---------|----------|-----------------|---------------|
//! | V1 | `TAP` verifier | Legacy allocations | JSON |
//! | V2 | `GraphTallyCollector` | New allocations (Horizon) | Protobuf + Base64 |
//!
//! The version is determined by the `is_legacy` flag on the allocation. Legacy
//! allocations were created before the Horizon upgrade and use the original TAP
//! verifier contract.
//!
//! # EIP-712 Domains
//!
//! Each receipt version uses a different EIP-712 domain for signing:
//!
//! - **V1**: `name="TAP", version="1", chainId=<chain>, verifyingContract=<legacy_verifier>`
//! - **V2**: `name="GraphTallyCollector", version="1", chainId=<chain>, verifyingContract=<verifier>`
//!
//! # Receipt Fields
//!
//! Common fields in both versions:
//! - `allocation_id` / `collection_id`: Identifies the allocation being paid
//! - `value`: Payment amount in GRT wei
//! - `timestamp_ns`: Creation time (nanoseconds since epoch)
//! - `nonce`: Random value to prevent replay attacks
//!
//! V2 adds:
//! - `payer`: Gateway's payment address
//! - `data_service`: Subgraph service contract address
//! - `service_provider`: Indexer's address
//!
//! # Usage
//!
//! ```ignore
//! let signer = ReceiptSigner::new(payer, key, chain_id, verifier, legacy_verifier, data_service);
//! let receipt = signer.create_receipt(allocation_id, indexer, fee, is_legacy)?;
//! let serialized = receipt.serialize(); // Send this to indexer
//! ```

use std::time::SystemTime;

use base64::{Engine as _, prelude::BASE64_STANDARD};
use prost::Message as _;
use rand::RngCore as _;
use thegraph_core::{
    AllocationId, CollectionId,
    alloy::{
        dyn_abi::Eip712Domain,
        primitives::{Address, U256},
        signers::local::PrivateKeySigner,
    },
};

#[derive(Debug, Clone)]
pub enum Receipt {
    V1(tap_graph::SignedReceipt),
    V2(tap_graph::v2::SignedReceipt),
}

impl Receipt {
    pub fn value(&self) -> u128 {
        match self {
            Receipt::V1(receipt) => receipt.message.value,
            Receipt::V2(receipt) => receipt.message.value,
        }
    }

    pub fn allocation(&self) -> Address {
        match self {
            Receipt::V1(receipt) => receipt.message.allocation_id,
            Receipt::V2(receipt) => {
                // TAP v2 receipts use collection ids which are 32 bytes.
                // For the Subgraph Service these are 20 byte allocation ids with zero padding.
                CollectionId::from(receipt.message.collection_id).as_address()
            }
        }
    }

    pub fn serialize(&self) -> String {
        match self {
            Receipt::V1(receipt) => {
                // V1 receipts use JSON serialization
                serde_json::to_string(receipt).expect("failed to serialize v1 receipt")
            }
            Receipt::V2(receipt) => self.serialize_v2(receipt),
        }
    }

    fn serialize_v2(&self, receipt: &tap_graph::v2::SignedReceipt) -> String {
        #[derive(prost::Message)]
        struct ReceiptMessage {
            #[prost(bytes, tag = "1")]
            collection_id: Vec<u8>,
            #[prost(bytes, tag = "2")]
            payer: Vec<u8>,
            #[prost(bytes, tag = "3")]
            data_service: Vec<u8>,
            #[prost(bytes, tag = "4")]
            service_provider: Vec<u8>,
            #[prost(uint64, tag = "5")]
            timestamp_ns: u64,
            #[prost(uint64, tag = "6")]
            nonce: u64,
            #[prost(message, optional, tag = "7")]
            value: Option<Uint128>,
        }
        #[derive(prost::Message)]
        struct Uint128 {
            #[prost(uint64, tag = "1")]
            high: u64,
            #[prost(uint64, tag = "2")]
            low: u64,
        }
        #[derive(prost::Message)]
        struct SignedReceipt {
            #[prost(message, optional, tag = "1")]
            message: Option<ReceiptMessage>,
            #[prost(bytes, tag = "2")]
            signature: Vec<u8>,
        }
        let receipt_message = ReceiptMessage {
            collection_id: receipt.message.collection_id.to_vec(),
            payer: receipt.message.payer.to_vec(),
            data_service: receipt.message.data_service.to_vec(),
            service_provider: receipt.message.service_provider.to_vec(),
            timestamp_ns: receipt.message.timestamp_ns,
            nonce: receipt.message.nonce,
            value: Some(Uint128 {
                high: (receipt.message.value >> 64) as u64,
                low: receipt.message.value as u64,
            }),
        };
        let signature_bytes = receipt.signature.as_bytes().to_vec();

        let signed_receipt = SignedReceipt {
            message: Some(receipt_message),
            signature: signature_bytes,
        };

        BASE64_STANDARD.encode(signed_receipt.encode_to_vec())
    }
}

pub struct ReceiptSigner {
    payer: Address,
    signer: PrivateKeySigner,
    v2_domain: Eip712Domain,
    v1_domain: Eip712Domain,
    data_service: Address,
}

impl ReceiptSigner {
    pub fn new(
        payer: Address,
        signer: PrivateKeySigner,
        chain_id: U256,
        verifying_contract: Address,
        legacy_verifying_contract: Address,
        data_service: Address,
    ) -> Self {
        let v2_domain = Eip712Domain {
            name: Some("GraphTallyCollector".into()),
            version: Some("1".into()),
            chain_id: Some(chain_id),
            verifying_contract: Some(verifying_contract),
            salt: None,
        };
        let v1_domain = Eip712Domain {
            name: Some("TAP".into()),
            version: Some("1".into()),
            chain_id: Some(chain_id),
            verifying_contract: Some(legacy_verifying_contract),
            salt: None,
        };
        Self {
            payer,
            signer,
            v2_domain,
            v1_domain,
            data_service,
        }
    }

    /// Create a v1 receipt for legacy allocations
    pub fn create_v1_receipt(
        &self,
        allocation: AllocationId,
        fee: u128,
    ) -> anyhow::Result<Receipt> {
        let nonce = rand::rng().next_u64();
        let timestamp_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to convert timestamp to ns"))?;
        let receipt = tap_graph::Receipt {
            allocation_id: allocation.0.0.into(),
            timestamp_ns,
            nonce,
            value: fee,
        };
        tap_graph::SignedReceipt::new(&self.v1_domain, receipt, &self.signer)
            .map(Receipt::V1)
            .map_err(|e| anyhow::anyhow!("failed to sign v1 receipt: {:?}", e))
    }

    /// Create a v2 receipt for new allocations
    pub fn create_v2_receipt(
        &self,
        allocation: AllocationId,
        indexer: Address,
        fee: u128,
    ) -> anyhow::Result<Receipt> {
        let nonce = rand::rng().next_u64();
        let timestamp_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to convert timestamp to ns"))?;
        let receipt = tap_graph::v2::Receipt {
            collection_id: CollectionId::from(allocation).0.into(),
            payer: self.payer,
            data_service: self.data_service,
            service_provider: indexer,
            timestamp_ns,
            nonce,
            value: fee,
        };
        tap_graph::v2::SignedReceipt::new(&self.v2_domain, receipt, &self.signer)
            .map(Receipt::V2)
            .map_err(|e| anyhow::anyhow!("failed to sign v2 receipt: {:?}", e))
    }

    /// Create a receipt based on whether the allocation is legacy or not
    pub fn create_receipt(
        &self,
        allocation: AllocationId,
        indexer: Address,
        fee: u128,
        is_legacy: bool,
    ) -> anyhow::Result<Receipt> {
        if is_legacy {
            self.create_v1_receipt(allocation, fee)
        } else {
            self.create_v2_receipt(allocation, indexer, fee)
        }
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::{
        allocation_id,
        alloy::{primitives::address, signers::local::PrivateKeySigner},
    };

    use super::*;

    fn create_test_signer() -> ReceiptSigner {
        let secret_key = PrivateKeySigner::from_slice(&[0xcd; 32]).expect("invalid secret key");
        ReceiptSigner::new(
            address!("1111111111111111111111111111111111111111"),
            secret_key,
            1.try_into().expect("invalid chain id"),
            address!("177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
            address!("277b557b12f22bb17a9d73dcc994d978dd6f5f89"), // legacy verifier
            address!("2222222222222222222222222222222222222222"),
        )
    }

    #[test]
    fn create_v2_receipt() {
        let signer = create_test_signer();
        let allocation = allocation_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
        let fee = 1000;

        let receipt = signer
            .create_receipt(
                allocation,
                address!("3333333333333333333333333333333333333333"),
                fee,
                false, // not legacy
            )
            .expect("failed to create v2 receipt");

        assert_eq!(receipt.value(), fee);
        assert_eq!(AllocationId::from(receipt.allocation()), allocation);
    }

    #[test]
    fn test_receipt_serialization() {
        let signer = create_test_signer();
        let allocation = allocation_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
        let fee = 1000;

        let receipt = signer
            .create_receipt(
                allocation,
                address!("3333333333333333333333333333333333333333"),
                fee,
                false, // not legacy
            )
            .expect("failed to create v2 receipt");

        let serialized = receipt.serialize();
        assert!(!serialized.is_empty());
        assert_eq!(receipt.value(), fee);
    }

    #[test]
    fn create_v1_receipt() {
        let signer = create_test_signer();
        let allocation = allocation_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
        let fee = 1000;

        let receipt = signer
            .create_receipt(
                allocation,
                address!("3333333333333333333333333333333333333333"),
                fee,
                true, // legacy
            )
            .expect("failed to create v1 receipt");

        assert_eq!(receipt.value(), fee);
        assert_eq!(AllocationId::from(receipt.allocation()), allocation);
    }
}
