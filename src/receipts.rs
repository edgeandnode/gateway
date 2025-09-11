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
pub struct Receipt(pub tap_graph::v2::SignedReceipt);

impl Receipt {
    pub fn value(&self) -> u128 {
        self.0.message.value
    }

    pub fn allocation(&self) -> Address {
        // TAP v2 receipts use collection ids which are 32 bytes.
        // For the Subgraph Service these are 20 byte allocation ids with zero padding.
        CollectionId::from(self.0.message.collection_id).as_address()
    }

    pub fn serialize(&self) -> String {
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
            collection_id: self.0.message.collection_id.to_vec(),
            payer: self.0.message.payer.to_vec(),
            data_service: self.0.message.data_service.to_vec(),
            service_provider: self.0.message.service_provider.to_vec(),
            timestamp_ns: self.0.message.timestamp_ns,
            nonce: self.0.message.nonce,
            value: Some(Uint128 {
                high: (self.0.message.value >> 64) as u64,
                low: self.0.message.value as u64,
            }),
        };
        let signature_bytes = self.0.signature.as_bytes().to_vec();

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
    domain: Eip712Domain,
    data_service: Address,
}

impl ReceiptSigner {
    pub fn new(
        payer: Address,
        signer: PrivateKeySigner,
        chain_id: U256,
        verifying_contract: Address,
        data_service: Address,
    ) -> Self {
        let domain = Eip712Domain {
            name: Some("GraphTallyCollector".into()),
            version: Some("1".into()),
            chain_id: Some(chain_id),
            verifying_contract: Some(verifying_contract),
            salt: None,
        };
        Self {
            payer,
            signer,
            domain,
            data_service,
        }
    }

    pub fn create_receipt(
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
        tap_graph::v2::SignedReceipt::new(&self.domain, receipt, &self.signer)
            .map(Receipt)
            .map_err(|e| anyhow::anyhow!("failed to sign v2 receipt: {:?}", e))
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
            )
            .expect("failed to create v2 receipt");

        let serialized = receipt.serialize();
        assert!(!serialized.is_empty());
        assert_eq!(receipt.value(), fee);
    }
}
