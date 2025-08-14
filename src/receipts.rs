use std::time::SystemTime;

use rand::RngCore;
use serde::Serialize;
use thegraph_core::{
    CollectionId,
    alloy::{
        dyn_abi::Eip712Domain,
        primitives::{Address, U256},
        signers::local::PrivateKeySigner,
    },
};

/// TAP v2 receipts for the Horizon upgrade
///
/// This gateway only generates and processes v2 receipts (collection-based)
#[derive(Debug, Clone)]
pub struct Receipt(pub tap_graph::v2::SignedReceipt);

impl Receipt {
    /// Get the fee value from the receipt
    pub fn value(&self) -> u128 {
        self.0.message.value
    }

    /// Get the collection identifier
    pub fn collection(&self) -> CollectionId {
        self.0.message.collection_id.into()
    }

    /// Serialize the receipt to JSON string
    pub fn serialize(&self) -> String {
        serde_json::to_string(&self.0).unwrap()
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
            name: Some("TAP".into()),
            version: Some("2".into()),
            chain_id: Some(chain_id),
            verifying_contract: Some(verifying_contract),
            salt: None,
        };

        Self {
            signer,
            v2_config: ReceiptConfig { domain: v2_domain },
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

        Ok(Receipt(signed))
    }

    pub fn payer_address(&self) -> Address {
        self.signer.address()
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::{
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
    fn create_v2_receipt() {
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
        assert_eq!(receipt.collection(), collection);
    }

    #[test]
    fn test_receipt_serialization() {
        let signer = create_test_signer();
        let collection =
            collection_id!("89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2a00000000000000000000000");
        let fee = 1000;

        let receipt = signer
            .create_receipt(
                collection,
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
