use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use alloy_primitives::{Address, U256};
use alloy_sol_types::Eip712Domain;
use ethers::signers::Wallet;
use eventuals::{Eventual, Ptr};
use rand::RngCore;
pub use receipts::{QueryStatus as ReceiptStatus, ReceiptPool};
use secp256k1::SecretKey;
use tap_core::eip_712_signed_message::EIP712SignedMessage;
use tap_core::tap_receipt::Receipt;
use tokio::sync::{Mutex, RwLock};

use gateway_common::types::{Indexing, GRT};

pub struct ReceiptSigner {
    signer: SecretKey,
    domain: Eip712Domain,
    allocations: RwLock<HashMap<Indexing, Address>>,
    legacy_signer: &'static SecretKey,
    legacy_indexers: Eventual<Ptr<HashSet<Address>>>,
    legacy_pools: RwLock<HashMap<Indexing, Arc<Mutex<ReceiptPool>>>>,
}

pub enum ScalarReceipt {
    Legacy(Vec<u8>),
    TAP(EIP712SignedMessage<Receipt>),
}

impl ScalarReceipt {
    pub fn allocation(&self) -> Address {
        match self {
            ScalarReceipt::Legacy(receipt) => Address::from_slice(&receipt[0..20]),
            ScalarReceipt::TAP(receipt) => receipt.message.allocation_id,
        }
    }

    pub fn serialize(&self) -> String {
        match self {
            ScalarReceipt::Legacy(receipt) => hex::encode(&receipt[..(receipt.len() - 32)]),
            ScalarReceipt::TAP(receipt) => serde_json::to_string(&receipt).unwrap(),
        }
    }
}

impl ReceiptSigner {
    pub async fn new(
        signer: SecretKey,
        chain_id: U256,
        verifier: Address,
        legacy_signer: &'static SecretKey,
        legacy_indexers: Eventual<Ptr<HashSet<Address>>>,
    ) -> Self {
        let _ = legacy_indexers.value().await;
        Self {
            signer,
            domain: Eip712Domain {
                name: Some("TAP".into()),
                version: Some("1".into()),
                chain_id: Some(chain_id),
                verifying_contract: Some(verifier),
                salt: None,
            },
            allocations: RwLock::default(),
            legacy_signer,
            legacy_indexers,
            legacy_pools: RwLock::default(),
        }
    }

    pub async fn create_receipt(&self, indexing: &Indexing, fee: &GRT) -> Option<ScalarReceipt> {
        if self
            .legacy_indexers
            .value_immediate()
            .unwrap_or_default()
            .contains(&indexing.indexer)
        {
            let legacy_pools = self.legacy_pools.read().await;
            let legacy_pool = legacy_pools.get(indexing)?;
            let mut legacy_pool = legacy_pool.lock().await;
            let locked_fee =
                primitive_types::U256::from_little_endian(&fee.0.raw_u256().as_le_bytes());
            let receipt = legacy_pool.commit(locked_fee).ok()?;
            return Some(ScalarReceipt::Legacy(receipt));
        }

        let allocation = *self.allocations.read().await.get(indexing)?;
        // TODO: risk management (cap on outstanding debts that proactively prevents sending receipt)
        let nonce = rand::thread_rng().next_u64();
        let timestamp_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .try_into()
            .unwrap();
        let receipt = Receipt {
            allocation_id: allocation.0 .0.into(),
            timestamp_ns,
            nonce,
            value: fee.0.as_u128().unwrap_or(0),
        };
        let wallet =
            Wallet::from_bytes(self.signer.as_ref()).expect("failed to prepare receipt wallet");
        let signed = EIP712SignedMessage::new(&self.domain, receipt, &wallet)
            .await
            .expect("failed to sign receipt");
        Some(ScalarReceipt::TAP(signed))
    }

    pub async fn record_receipt(
        &self,
        indexing: &Indexing,
        receipt: &ScalarReceipt,
        status: ReceiptStatus,
    ) {
        match receipt {
            ScalarReceipt::Legacy(receipt) => {
                let legacy_pool = self.legacy_pools.read().await;
                let mut legacy_pool = match legacy_pool.get(indexing) {
                    Some(legacy_pool) => legacy_pool.lock().await,
                    None => return,
                };
                legacy_pool.release(receipt, status);
            }
            ScalarReceipt::TAP(_) => {
                // TODO: TAP collateral management
            }
        }
    }

    pub async fn update_allocations(&self, indexings: HashMap<Indexing, Address>) {
        for (indexing, allocation) in &indexings {
            let legacy_pool = self.get(indexing).await;
            let mut legacy_pool = legacy_pool.lock().await;
            // remove stale allocations
            for old_allocation in legacy_pool
                .addresses()
                .into_iter()
                .filter(|a| a != *allocation)
            {
                legacy_pool.remove_allocation(&old_allocation);
            }
            // add allocation, if not already present
            if !legacy_pool.contains_allocation(allocation) {
                legacy_pool.add_allocation(*self.legacy_signer, *allocation.0);
            }
        }

        let mut allocations = self.allocations.write().await;
        // remove stale allocations
        allocations.retain(|k, _| indexings.contains_key(k));
        // update allocations
        for (indexing, allocation) in indexings {
            allocations.insert(indexing, allocation);
        }
    }

    async fn get(&self, indexing: &Indexing) -> Arc<Mutex<ReceiptPool>> {
        if let Some(pool) = self.legacy_pools.read().await.get(indexing) {
            return pool.clone();
        }
        let mut legacy_pools = self.legacy_pools.write().await;
        match legacy_pools.entry(*indexing) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let pool = Arc::new(Mutex::default());
                entry.insert(pool.clone());
                pool
            }
        }
    }
}
