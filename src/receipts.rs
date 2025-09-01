use std::time::SystemTime;

use base64::{Engine as _, engine::general_purpose};
use prost::Message;
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

/// TAP v2 receipts for the Horizon upgrade
///
/// This gateway only generates v2 receipts, but can process v1 receipts as well.
#[derive(Debug, Clone)]
pub struct Receipt(pub tap_graph::v2::SignedReceipt);

impl Receipt {
    /// Get the fee value from the receipt
    pub fn value(&self) -> u128 {
        self.0.message.value
    }

    /// Get the allocation identifier
    /// TAP v2 receipts use collection ids which are 32 bytes.
    /// For the Subgraph Service these are 20 byte allocation ids with zero padding.
    pub fn allocation(&self) -> Address {
        CollectionId::from(self.0.message.collection_id).as_address()
    }

    /// Serialize the receipt to base64-encoded protobuf format for V2 compatibility
    pub fn serialize(&self) -> String {
        // Convert tap_graph::v2::SignedReceipt to protobuf format
        let protobuf_receipt: tap_aggregator::grpc::v2::SignedReceipt = self.0.clone().into();

        // Encode to protobuf bytes
        let bytes = protobuf_receipt.encode_to_vec();

        // Base64 encode the bytes
        let serialized = general_purpose::STANDARD.encode(&bytes);

        // DEBUG: Log receipt serialization details
        tracing::debug!(
            serialized_preview = &serialized[..serialized.len().min(100)],
            serialized_length = serialized.len(),
            protobuf_bytes_length = bytes.len(),
            "TAP receipt serialized format"
        );

        serialized
    }
}

impl Serialize for Receipt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// Configuration for receipt creation
#[derive(Debug, Clone)]
pub struct ReceiptConfig {
    pub domain: Eip712Domain,
}

pub struct ReceiptSigner {
    signer: PrivateKeySigner,
    v2_config: ReceiptConfig,
}

impl ReceiptSigner {
    pub fn new(signer: PrivateKeySigner, chain_id: U256, verifying_contract: Address) -> Self {
        let v2_domain = Eip712Domain {
            name: Some("GraphTallyCollector".into()),
            version: Some("1".into()),
            chain_id: Some(chain_id),
            verifying_contract: Some(verifying_contract),
            salt: None,
        };

        Self {
            signer,
            v2_config: ReceiptConfig { domain: v2_domain },
        }
    }

    /// Create a v2 receipt - ONLY method for generating receipts
    pub fn create_receipt(
        &self,
        allocation: AllocationId,
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
            collection_id: CollectionId::from(allocation).0.into(),
            payer,
            data_service,
            service_provider,
            timestamp_ns,
            nonce,
            value: fee,
        };

        // DEBUG: Log all receipt fields before signing
        tracing::debug!(
            collection_id = ?receipt.collection_id,
            allocation_id = ?allocation,
            payer = ?receipt.payer,
            data_service = ?receipt.data_service,
            service_provider = ?receipt.service_provider,
            timestamp_ns = receipt.timestamp_ns,
            nonce = receipt.nonce,
            value = receipt.value,
            "TAP receipt fields before signing"
        );

        // DEBUG: Log EIP-712 domain components
        tracing::debug!(
            domain_name = ?self.v2_config.domain.name,
            domain_version = ?self.v2_config.domain.version,
            domain_chain_id = ?self.v2_config.domain.chain_id,
            domain_verifying_contract = ?self.v2_config.domain.verifying_contract,
            "TAP EIP-712 domain components"
        );

        let signed =
            tap_graph::v2::SignedReceipt::new(&self.v2_config.domain, receipt, &self.signer)
                .map_err(|e| anyhow::anyhow!("failed to sign v2 receipt: {:?}", e))?;

        // DEBUG: Log the signature being generated
        tracing::debug!(
            signature = ?signed.signature,
            signature_bytes = ?signed.signature.as_bytes(),
            expected_signer = ?self.signer.address(),
            "TAP receipt signature generated"
        );

        Ok(Receipt(signed))
    }

    pub fn payer_address(&self) -> Address {
        self.signer.address()
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
            secret_key,
            1.try_into().expect("invalid chain id"),
            address!("177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
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
                fee,
                address!("1111111111111111111111111111111111111111"), // payer
                address!("2222222222222222222222222222222222222222"), // data_service
                address!("3333333333333333333333333333333333333333"), // service_provider
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
                fee,
                address!("1111111111111111111111111111111111111111"),
                address!("2222222222222222222222222222222222222222"),
                address!("3333333333333333333333333333333333333333"),
            )
            .expect("failed to create v2 receipt");

        let serialized = receipt.serialize();
        assert!(!serialized.is_empty());
        assert_eq!(receipt.value(), fee);
    }
}
