use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use alloy_primitives::{Address, U256};
use alloy_sol_types::Eip712Domain;
use ethers::{core::k256::ecdsa::SigningKey, signers::Wallet};
use gateway_common::ttl_hash_map::TtlHashMap;
use rand::RngCore;
pub use receipts::QueryStatus as ReceiptStatus;
use receipts::ReceiptPool;
use secp256k1::SecretKey;
use tap_core::{receipt::Receipt, signed_message::EIP712SignedMessage};
use thegraph_core::types::DeploymentId;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub enum ScalarReceipt {
    Legacy(u128, Vec<u8>),
    TAP(EIP712SignedMessage<Receipt>),
}

impl ScalarReceipt {
    pub fn grt_value(&self) -> u128 {
        match self {
            ScalarReceipt::Legacy(value, _) => *value,
            ScalarReceipt::TAP(receipt) => receipt.message.value,
        }
    }

    pub fn allocation(&self) -> Address {
        match self {
            ScalarReceipt::Legacy(_, receipt) => Address::from_slice(&receipt[0..20]),
            ScalarReceipt::TAP(receipt) => receipt.message.allocation_id,
        }
    }

    pub fn serialize(&self) -> String {
        match self {
            ScalarReceipt::Legacy(_, receipt) => hex::encode(&receipt[..(receipt.len() - 32)]),
            ScalarReceipt::TAP(receipt) => serde_json::to_string(&receipt).unwrap(),
        }
    }

    pub fn header_name(&self) -> &'static str {
        match self {
            ScalarReceipt::Legacy(_, _) => "Scalar-Receipt",
            ScalarReceipt::TAP(_) => "Tap-Receipt",
        }
    }
}

/// Scalar TAP signer.
struct TapSigner {
    signer: Wallet<SigningKey>,
    domain: Eip712Domain,
}

impl TapSigner {
    /// Creates a new `TapSigner`.
    fn new(signer: SecretKey, chain_id: U256, verifying_contract: Address) -> Self {
        let signer = Wallet::from_bytes(signer.as_ref()).expect("failed to prepare receipt wallet");

        Self {
            signer,
            domain: Eip712Domain {
                name: Some("TAP".into()),
                version: Some("1".into()),
                chain_id: Some(chain_id),
                verifying_contract: Some(verifying_contract),
                salt: None,
            },
        }
    }

    /// Creates a new receipt for the given allocation and fee.
    fn create_receipt(
        &self,
        allocation: Address,
        fee: u128,
    ) -> anyhow::Result<EIP712SignedMessage<Receipt>> {
        // Nonce generated with CSPRNG (ChaCha12), to avoid collision with receipts generated by
        // other gateway processes.
        // See https://docs.rs/rand/latest/rand/rngs/index.html#our-generators.
        let nonce = rand::thread_rng().next_u64();

        let timestamp_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .try_into()
            .map_err(|_| anyhow::anyhow!("failed to convert timestamp to ns"))?;

        let receipt = Receipt {
            allocation_id: allocation.0 .0.into(),
            timestamp_ns,
            nonce,
            value: fee,
        };
        let signed = EIP712SignedMessage::new(&self.domain, receipt, &self.signer)
            .map_err(|e| anyhow::anyhow!("failed to sign receipt: {:?}", e))?;

        Ok(signed)
    }
}

/// Legacy Scalar signer.
struct LegacySigner {
    secret_key: &'static SecretKey,
    receipt_pools: RwLock<TtlHashMap<Address, Arc<Mutex<ReceiptPool>>>>,
}

impl LegacySigner {
    /// Creates a new `LegacySigner`.
    fn new(secret_key: &'static SecretKey) -> Self {
        let legacy_pool_ttl = Duration::from_secs(12 * 60 * 60); // 12 hours

        Self {
            secret_key,
            receipt_pools: RwLock::new(TtlHashMap::with_ttl(legacy_pool_ttl)),
        }
    }

    /// Creates a new receipt for the given allocation and fee.
    async fn create_receipt(
        &self,
        allocation: Address,
        fee: u128,
    ) -> anyhow::Result<(u128, Vec<u8>)> {
        // Get the pool for the allocation
        let receipt_pool = self.receipt_pools.read().await.get(&allocation).cloned();

        // If the pool for the allocation exists, use it. Otherwise, create a new pool.
        let receipt = match receipt_pool {
            Some(pool) => {
                let mut pool = pool.lock().await;
                pool.commit(self.secret_key, 0.into())
            }
            None => {
                let mut pool = ReceiptPool::new(allocation.0 .0);
                let receipt = pool.commit(self.secret_key, 0.into());

                let mut write_guard = self.receipt_pools.write().await;
                write_guard.insert(allocation, Arc::new(Mutex::new(pool)));

                receipt
            }
        }
        .map_err(|e| anyhow::anyhow!("failed to sign legacy receipt: {:?}", e))?;

        Ok((fee, receipt))
    }

    /// Record the receipt status and release it from the pool.
    async fn record_receipt(&self, allocation: Address, receipt: &[u8], status: ReceiptStatus) {
        let legacy_pool = self.receipt_pools.read().await;
        if let Some(legacy_pool) = legacy_pool.get(&allocation) {
            legacy_pool.lock().await.release(receipt, status);
        };
    }
}

/// ReceiptSigner is responsible for creating receipts for indexing requests.
pub struct ReceiptSigner {
    tap: TapSigner,
    legacy: LegacySigner,

    /// Mapping of the largest allocation for each indexer-deployment pair.
    // TODO: Remove this once the network service is integrated and the largest allocation can be
    //  accessed via the `Indexing` instance.
    largest_allocations: RwLock<HashMap<(Address, DeploymentId), Address>>,
}

impl ReceiptSigner {
    /// Creates a new `ReceiptSigner`.
    pub fn new(
        signer: SecretKey,
        chain_id: U256,
        verifier: Address,
        legacy_signer: &'static SecretKey,
    ) -> Self {
        Self {
            tap: TapSigner::new(signer, chain_id, verifier),
            legacy: LegacySigner::new(legacy_signer),
            largest_allocations: Default::default(),
        }
    }

    /// Creates a new Scalar TAP receipt for the given allocation and fee.
    pub async fn create_receipt(
        &self,
        indexer: Address,
        deployment: DeploymentId,
        fee: u128,
    ) -> anyhow::Result<ScalarReceipt> {
        // Get the largest allocation for the given indexer-deployment pair.
        // TODO: Remove this once the network service is integrated and the largest allocation can
        //  be accessed via the `Indexing` instance.
        let allocation = self
            .largest_allocations
            .read()
            .await
            .get(&(indexer, deployment))
            .cloned()
            .ok_or(anyhow::anyhow!("indexing allocation address not found"))?;

        self.tap
            .create_receipt(allocation, fee)
            .map(ScalarReceipt::TAP)
    }

    /// Creates a new Scalar legacy receipt for the given allocation and fee.
    pub async fn create_legacy_receipt(
        &self,
        indexer: Address,
        deployment: DeploymentId,
        fee: u128,
    ) -> anyhow::Result<ScalarReceipt> {
        // Get the largest allocation for the given indexer-deployment pair.
        // TODO: Remove this once the network service is integrated and the largest allocation can
        //  be accessed via the `Indexing` instance.
        let allocation = self
            .largest_allocations
            .read()
            .await
            .get(&(indexer, deployment))
            .cloned()
            .ok_or(anyhow::anyhow!("indexing allocation address not found"))?;

        self.legacy
            .create_receipt(allocation, fee)
            .await
            .map(|(fee, receipt)| ScalarReceipt::Legacy(fee, receipt))
    }

    /// Record the receipt status and release it from the pool.
    pub async fn record_receipt(
        &self,
        indexer: Address,
        deployment: DeploymentId,
        receipt: &ScalarReceipt,
        status: ReceiptStatus,
    ) {
        if let ScalarReceipt::Legacy(_, receipt) = receipt {
            // Get the largest allocation for the given indexer-deployment pair.
            // TODO: Remove this once the network service is integrated and the largest allocation
            //  can be accessed via the `Indexing` instance.
            let allocation = match self
                .largest_allocations
                .read()
                .await
                .get(&(indexer, deployment))
                .cloned()
            {
                Some(address) => address,
                None => {
                    tracing::debug!(
                        error = "indexing allocation address not found",
                        "failed to record receipt status"
                    );
                    return;
                }
            };

            self.legacy
                .record_receipt(allocation, receipt, status)
                .await;
        }
    }

    /// Update the largest allocation for the given indexings (indexer-deployment pairs).
    pub async fn update_allocations(&self, indexings: &HashMap<(Address, DeploymentId), Address>) {
        let mut allocations = self.largest_allocations.write().await;
        allocations.retain(|k, _| indexings.contains_key(k));
        for (indexing, allocation) in indexings {
            allocations.insert(*indexing, *allocation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper to get an [`Address`] from a given string.
    fn parse_address(addr: impl AsRef<str>) -> Address {
        addr.as_ref().parse().expect("Invalid address")
    }

    /// Test helper to get a [`DeploymentId`] from a given string.
    fn parse_deployment_id(deployment_id: impl AsRef<str>) -> DeploymentId {
        deployment_id
            .as_ref()
            .parse()
            .expect("Invalid deployment ID")
    }

    mod legacy {
        use super::*;

        #[tokio::test]
        async fn create_receipt() {
            //* Given
            let secret_key = Box::leak(Box::new(
                SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key"),
            ));

            let signer = LegacySigner::new(secret_key);

            // let indexer = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
            // let deployment = parse_deployment_id("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
            let largest_allocation = parse_address("0x89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
            let fee = 1000;

            //* When
            let res = signer.create_receipt(largest_allocation, fee).await;

            //* Then
            let receipt = res.expect("failed to create legacy receipt");

            assert_eq!(receipt.0, fee);
            assert!(!receipt.1.is_empty());
        }

        #[tokio::test]
        async fn create_receipt_with_preexisting_pool() {
            //* Given
            let secret_key = Box::leak(Box::new(
                SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key"),
            ));

            let signer = LegacySigner::new(secret_key);

            let largest_allocation = parse_address("0x89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
            let fee = 1000;

            // Pre-condition: Create a receipt so the pool for the allocation exists
            let _ = signer.create_receipt(largest_allocation, fee).await;

            //* When
            let res = signer.create_receipt(largest_allocation, fee).await;

            //* Then
            let receipt = res.expect("failed to create legacy receipt");

            assert_eq!(receipt.0, fee);
            assert!(!receipt.1.is_empty());
        }
    }

    mod tap {
        use super::*;

        #[tokio::test]
        async fn create_receipt() {
            //* Given
            let secret_key = SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key");
            let signer = TapSigner::new(
                secret_key,
                1.try_into().expect("invalid chain id"),
                parse_address("0x177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
            );

            let allocation = parse_address("0x89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");
            let fee = 1000;

            //* When
            let res = signer.create_receipt(allocation, fee);

            //* Then
            let receipt = res.expect("failed to create tap receipt");

            assert_eq!(receipt.message.value, fee);
        }
    }

    #[tokio::test]
    async fn create_legacy_receipt() {
        //* Given
        let tap_secret_key = SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key");
        let legacy_secret_key = Box::leak(Box::new(
            SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key"),
        ));

        let signer = ReceiptSigner::new(
            tap_secret_key,
            1.try_into().expect("invalid chain id"),
            parse_address("0x177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
            legacy_secret_key,
        );

        // Indexing
        let indexer = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
        let deployment = parse_deployment_id("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
        let largest_allocation = parse_address("0x89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");

        let indexings = HashMap::from([((indexer, deployment), largest_allocation)]);

        // Update the receipt signer's largest allocations table
        signer.update_allocations(&indexings).await;

        let fee = 1000;

        //* When
        let res = signer.create_legacy_receipt(indexer, deployment, fee).await;

        //* Then
        let receipt = res.expect("failed to create legacy receipt");
        assert!(matches!(receipt, ScalarReceipt::Legacy(_, _)));
    }

    #[tokio::test]
    async fn create_tap_receipt() {
        //* Given
        let tap_secret_key = SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key");
        let legacy_secret_key = Box::leak(Box::new(
            SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key"),
        ));

        let signer = ReceiptSigner::new(
            tap_secret_key,
            1.try_into().expect("invalid chain id"),
            parse_address("0x177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
            legacy_secret_key,
        );

        // Indexings
        let indexer = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
        let deployment = parse_deployment_id("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
        let largest_allocation = parse_address("0x89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");

        let indexings = HashMap::from([((indexer, deployment), largest_allocation)]);

        // Update the receipt signer largest allocations table
        signer.update_allocations(&indexings).await;

        let fee = 1000;

        //* When
        let res = signer.create_receipt(indexer, deployment, fee).await;

        //* Then
        let receipt = res.expect("failed to create tap receipt");
        assert!(matches!(receipt, ScalarReceipt::TAP(_)));
    }

    #[tokio::test]
    async fn fail_creating_legacy_receipt_unknown_indexing() {
        //* Given
        let tap_secret_key = SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key");
        let legacy_secret_key = Box::leak(Box::new(
            SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key"),
        ));

        let signer = ReceiptSigner::new(
            tap_secret_key,
            1.try_into().expect("invalid chain id"),
            parse_address("0x177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
            legacy_secret_key,
        );

        // Indexing
        let indexer = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
        let deployment = parse_deployment_id("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
        // let largest_allocation = parse_address("0x89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");

        // Update the receipt signer's largest allocations table (no indexings)
        signer.update_allocations(&HashMap::new()).await;

        let fee = 1000;

        //* When
        let res = signer.create_legacy_receipt(indexer, deployment, fee).await;

        //* Then
        let receipt = res.expect_err("legacy receipt creation should fail");
        assert!(receipt
            .to_string()
            .contains("indexing allocation address not found"));
    }

    #[tokio::test]
    async fn fail_creating_tap_receipt_unknown_indexing() {
        //* Given
        let tap_secret_key = SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key");
        let legacy_secret_key = Box::leak(Box::new(
            SecretKey::from_slice(&[0xcd; 32]).expect("invalid secret key"),
        ));

        let signer = ReceiptSigner::new(
            tap_secret_key,
            1.try_into().expect("invalid chain id"),
            parse_address("0x177b557b12f22bb17a9d73dcc994d978dd6f5f89"),
            legacy_secret_key,
        );

        // Indexings
        let indexer = parse_address("0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491");
        let deployment = parse_deployment_id("QmaqcZxm6gcgWhWpQ88YKDm1keJDMpNxNGwtEDvjrjjNKh");
        // let largest_allocation = parse_address("0x89b23fea4e46d40e8a4c6cca723e2a03fdd4bec2");

        // Update the receipt signer's largest allocations table (no indexings)
        signer.update_allocations(&HashMap::new()).await;

        let fee = 1000;

        //* When
        let res = signer.create_receipt(indexer, deployment, fee).await;

        //* Then
        let receipt = res.expect_err("legacy receipt creation should fail");
        assert!(receipt
            .to_string()
            .contains("indexing allocation address not found"));
    }
}
