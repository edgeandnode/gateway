use std::time::SystemTime;

use rand::RngCore;
use serde::Serialize;
use thegraph_core::{
    AllocationId, CollectionId,
    alloy::{
        dyn_abi::Eip712Domain,
        primitives::{Address, U256},
        signers::local::PrivateKeySigner,
    },
};

/// Abstraction over TAP receipts that supports both v1 and v2 formats
///
/// Design:
/// - Gateway ONLY generates v2 receipts (collection-based)
/// - Gateway CAN process v1 receipts (allocation-based) for backward compatibility
/// - This horizon gateway focuses on v2 generation while maintaining v1 processing capability
#[derive(Debug, Clone)]
pub enum Receipt {
    #[allow(dead_code)] // Used for processing existing v1 receipts
    V1(V1Receipt), // For processing existing v1 receipts from indexers
    V2(tap_graph::v2::SignedReceipt), // For generation and processing
}

// Use tap_graph v1 types directly (should be available at root level)
pub use tap_graph::{Receipt as V1ReceiptMessage, SignedReceipt as V1Receipt};

impl Receipt {
    /// Get the fee value from either receipt version
    pub fn value(&self) -> u128 {
        match self {
            Receipt::V1(receipt) => receipt.message.value,
            Receipt::V2(receipt) => receipt.message.value,
        }
    }

    /// Get the collection identifier  
    /// For v1: converts allocation_id to CollectionId
    /// For v2: returns the collection_id directly
    pub fn collection(&self) -> CollectionId {
        match self {
            Receipt::V1(receipt) => receipt.message.allocation_id.into(),
            Receipt::V2(receipt) => receipt.message.collection_id.into(),
        }
    }

    /// Get the allocation ID
    /// For v1: returns allocation_id directly  
    /// For v2: converts collection_id to AllocationId
    #[allow(dead_code)] // Used when processing v1 receipts
    pub fn allocation(&self) -> AllocationId {
        match self {
            Receipt::V1(receipt) => receipt.message.allocation_id.into(),
            Receipt::V2(receipt) => CollectionId::from(receipt.message.collection_id).into(),
        }
    }

    /// Serialize the receipt to JSON string
    pub fn serialize(&self) -> String {
        match self {
            Receipt::V1(receipt) => serde_json::to_string(receipt).unwrap(),
            Receipt::V2(receipt) => serde_json::to_string(receipt).unwrap(),
        }
    }

    /// Get receipt version for debugging/logging
    #[allow(dead_code)] // Used for debugging when both receipt types are present
    pub fn version(&self) -> &'static str {
        match self {
            Receipt::V1(_) => "v1",
            Receipt::V2(_) => "v2",
        }
    }

    /// Get payer address (only available for v2 receipts)
    #[allow(dead_code)] // Used for v2 receipt processing
    pub fn payer(&self) -> Option<Address> {
        match self {
            Receipt::V1(_) => None, // v1 receipts don't have explicit payer field
            Receipt::V2(receipt) => Some(receipt.message.payer),
        }
    }

    /// Get data service address (only available for v2 receipts)
    #[allow(dead_code)] // Used for v2 receipt processing
    pub fn data_service(&self) -> Option<Address> {
        match self {
            Receipt::V1(_) => None,
            Receipt::V2(receipt) => Some(receipt.message.data_service),
        }
    }

    /// Get service provider address (only available for v2 receipts)
    #[allow(dead_code)] // Used for v2 receipt processing
    pub fn service_provider(&self) -> Option<Address> {
        match self {
            Receipt::V1(_) => None,
            Receipt::V2(receipt) => Some(receipt.message.service_provider),
        }
    }

    /// Check if this is a v1 receipt
    #[allow(dead_code)] // Used when both receipt types are present
    pub fn is_v1(&self) -> bool {
        matches!(self, Receipt::V1(_))
    }

    /// Check if this is a v2 receipt  
    #[allow(dead_code)] // Used when both receipt types are present
    pub fn is_v2(&self) -> bool {
        matches!(self, Receipt::V2(_))
    }

    /// Parse a receipt from JSON string, attempting both v1 and v2 formats
    #[allow(dead_code)] // Used for processing receipts from indexers
    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        // Try v2 first (current generation format)
        if let Ok(v2_receipt) = serde_json::from_str::<tap_graph::v2::SignedReceipt>(json) {
            return Ok(Receipt::V2(v2_receipt));
        }

        // Try v1 format for backwards compatibility
        if let Ok(v1_receipt) = serde_json::from_str::<V1Receipt>(json) {
            return Ok(Receipt::V1(v1_receipt));
        }

        Err(anyhow::anyhow!(
            "Failed to parse receipt as either v1 or v2 format"
        ))
    }
}

impl Serialize for Receipt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Receipt::V1(receipt) => receipt.serialize(serializer),
            Receipt::V2(receipt) => receipt.serialize(serializer),
        }
    }
}

/// Receipt version configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptVersion {
    #[allow(dead_code)] // Used when v1 support is needed
    V1, // Read-only support for processing existing receipts
    V2, // Current generation and processing version
}

/// Configuration for receipt creation
#[derive(Debug, Clone)]
pub struct ReceiptConfig {
    #[allow(dead_code)] // Used when multiple versions are configured
    pub version: ReceiptVersion,
    pub domain: Eip712Domain,
}

pub struct ReceiptSigner {
    signer: PrivateKeySigner,
    v2_config: ReceiptConfig,
}

impl ReceiptSigner {
    pub fn new(signer: PrivateKeySigner, chain_id: U256, verifying_contract: Address) -> Self {
        let v2_domain = Eip712Domain {
            name: Some("TAP".into()),
            version: Some("2".into()),
            chain_id: Some(chain_id),
            verifying_contract: Some(verifying_contract),
            salt: None,
        };

        Self {
            signer,
            v2_config: ReceiptConfig {
                version: ReceiptVersion::V2,
                domain: v2_domain,
            },
        }
    }

    /// Create a v2 receipt (collection-based) - ONLY method for generating receipts
    pub fn create_receipt(
        &self,
        collection: CollectionId,
        fee: u128,
        payer: Address,
        data_service: Address,
        service_provider: Address,
    ) -> anyhow::Result<Receipt> {
        let nonce = rand::rng().next_u64();
        let timestamp_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to convert timestamp to ns"))?;

        let receipt = tap_graph::v2::Receipt {
            collection_id: collection.0.into(),
            payer,
            data_service,
            service_provider,
            timestamp_ns,
            nonce,
            value: fee,
        };

        let signed =
            tap_graph::v2::SignedReceipt::new(&self.v2_config.domain, receipt, &self.signer)
                .map_err(|e| anyhow::anyhow!("failed to sign v2 receipt: {:?}", e))?;

        Ok(Receipt::V2(signed))
    }

    pub fn payer_address(&self) -> Address {
        self.signer.address()
    }

    /// Get the generation version (always v2 - we only generate v2 receipts)
    #[allow(dead_code)] // Used for debugging and configuration validation
    pub fn generation_version(&self) -> ReceiptVersion {
        ReceiptVersion::V2
    }

    /// Check if the gateway can process v1 receipts (read-only)
    #[allow(dead_code)] // Used for capability checking
    pub fn can_process_v1(&self) -> bool {
        true // We can always process/deserialize v1 receipts
    }

    /// Check if the gateway can generate v2 receipts
    #[allow(dead_code)] // Used for capability checking
    pub fn can_generate_v2(&self) -> bool {
        true // We always generate v2 receipts
    }
}

/// Utility functions for receipt processing
impl Receipt {
    /// Convert a v1 receipt to a format compatible with v2 processing
    /// This is useful when we need to process existing v1 receipts in a v2-compatible way
    #[allow(dead_code)] // Used when processing mixed receipt types
    pub fn normalize_for_processing(&self) -> (CollectionId, u128) {
        match self {
            Receipt::V1(receipt) => {
                // Convert allocation to collection format
                let collection = receipt.message.allocation_id.into();
                (collection, receipt.message.value)
            }
            Receipt::V2(receipt) => {
                let collection = receipt.message.collection_id.into();
                (collection, receipt.message.value)
            }
        }
    }

    /// Create a v1 receipt for testing/processing purposes (not for generation)
    #[allow(dead_code)] // Used for testing and processing existing v1 receipts
    pub fn create_v1_for_processing(
        allocation_id: AllocationId,
        value: u128,
        timestamp_ns: u64,
        nonce: u64,
    ) -> Self {
        // Create the v1 receipt message
        let receipt_message = V1ReceiptMessage {
            allocation_id: allocation_id.0.into(), // Convert AllocationId to Address
            value,
            timestamp_ns,
            nonce,
        };

        // Note: For testing purposes, we create an unsigned receipt
        // In practice, v1 receipts from indexers would be properly signed
        use thegraph_core::alloy::signers::Signature;
        let signed_receipt = V1Receipt {
            message: receipt_message,
            signature: Signature::from_bytes_and_parity(&[0u8; 64], false), // Placeholder signature for testing
        };

        Receipt::V1(signed_receipt)
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::{
        allocation_id,
        alloy::{primitives::address, signers::local::PrivateKeySigner},
        collection_id,
    };

    use super::*;

    fn create_test_signer() -> ReceiptSigner {
        let secret_key = PrivateKeySigner::from_slice(&[0xcd; 32]).expect("invalid secret key");
        ReceiptSigner::new(
            secret_key,
            1.try_into().expect("invalid chain id"),
            address!("177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
        )
    }

    #[test]
    fn create_v2_receipt_only() {
        let signer = create_test_signer();
        let collection =
            collection_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2a00000000000000000000000");
        let fee = 1000;

        let receipt = signer
            .create_receipt(
                collection,
                fee,
                address!("1111111111111111111111111111111111111111"), // payer
                address!("2222222222222222222222222222222222222222"), // data_service
                address!("3333333333333333333333333333333333333333"), // service_provider
            )
            .expect("failed to create v2 receipt");

        assert_eq!(receipt.value(), fee);
        assert_eq!(receipt.version(), "v2");
        assert_eq!(receipt.collection(), collection);
        assert!(receipt.is_v2());
        assert!(!receipt.is_v1());
    }

    #[test]
    fn process_v1_receipt() {
        let allocation = allocation_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
        let fee = 1000;
        let timestamp = 1234567890;
        let nonce = 42;

        let v1_receipt = Receipt::create_v1_for_processing(allocation, fee, timestamp, nonce);

        assert_eq!(v1_receipt.value(), fee);
        assert_eq!(v1_receipt.version(), "v1");
        assert_eq!(v1_receipt.allocation(), allocation);
        assert!(v1_receipt.is_v1());
        assert!(!v1_receipt.is_v2());

        // v1 receipts don't have v2-specific fields
        assert_eq!(v1_receipt.payer(), None);
        assert_eq!(v1_receipt.data_service(), None);
        assert_eq!(v1_receipt.service_provider(), None);
    }

    #[test]
    fn normalize_receipts_for_processing() {
        let signer = create_test_signer();
        let collection =
            collection_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2a00000000000000000000000");
        let allocation = allocation_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
        let fee = 1000;

        // Create v2 receipt
        let v2_receipt = signer
            .create_receipt(
                collection,
                fee,
                address!("1111111111111111111111111111111111111111"),
                address!("2222222222222222222222222222222222222222"),
                address!("3333333333333333333333333333333333333333"),
            )
            .expect("failed to create v2 receipt");

        // Create v1 receipt for processing
        let v1_receipt = Receipt::create_v1_for_processing(allocation, fee, 1234567890, 42);

        // Both should normalize to the same processing format
        let (v2_collection, v2_value) = v2_receipt.normalize_for_processing();
        let (v1_collection, v1_value) = v1_receipt.normalize_for_processing();

        assert_eq!(v2_value, fee);
        assert_eq!(v1_value, fee);
        assert_eq!(v2_collection, collection);

        // v1 allocation should convert to a collection (though different from v2)
        let expected_collection: CollectionId = allocation.into();
        assert_eq!(v1_collection, expected_collection);
    }

    #[test]
    fn test_receipt_capabilities() {
        let signer = create_test_signer();

        assert!(
            signer.can_process_v1(),
            "Should be able to process v1 receipts"
        );
        assert!(
            signer.can_generate_v2(),
            "Should be able to generate v2 receipts"
        );
        assert_eq!(signer.generation_version(), ReceiptVersion::V2);
    }

    #[test]
    fn test_receipt_parsing() {
        let signer = create_test_signer();
        let collection =
            collection_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2a00000000000000000000000");
        let fee = 1000;

        // Create and serialize a v2 receipt
        let v2_receipt = signer
            .create_receipt(
                collection,
                fee,
                address!("1111111111111111111111111111111111111111"),
                address!("2222222222222222222222222222222222222222"),
                address!("3333333333333333333333333333333333333333"),
            )
            .expect("failed to create v2 receipt");

        let serialized = v2_receipt.serialize();

        // Should be able to parse it back
        let parsed = Receipt::from_json(&serialized).expect("failed to parse receipt");
        assert!(parsed.is_v2());
        assert_eq!(parsed.value(), fee);
    }
}
