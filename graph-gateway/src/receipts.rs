use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use alloy_primitives::Address;
use tokio::sync::{Mutex, RwLock};

pub use indexer_selection::receipts::QueryStatus as ReceiptStatus;
use indexer_selection::{
    receipts::{BorrowFail, ReceiptPool},
    Indexing, SecretKey,
};
use prelude::GRT;

pub struct ReceiptSigner {
    // TODO: When legacy (non-TAP) Scalar is removed, this should contain the only owned reference
    // to the SignerKey. This will resolve https://github.com/edgeandnode/graph-gateway/issues/13.
    signer_key: SecretKey,
    legacy_pools: RwLock<HashMap<Indexing, Arc<Mutex<ReceiptPool>>>>,
}

impl ReceiptSigner {
    pub fn new(signer_key: SecretKey) -> Self {
        Self {
            signer_key,
            legacy_pools: RwLock::default(),
        }
    }

    pub async fn create_receipt(
        &self,
        indexing: &Indexing,
        fee: GRT,
    ) -> Result<Vec<u8>, BorrowFail> {
        let legacy_pools = self.legacy_pools.read().await;
        let legacy_pool = legacy_pools.get(indexing).ok_or(BorrowFail::NoAllocation)?;
        let mut legacy_pool = legacy_pool.lock().await;
        legacy_pool.commit(fee.shift::<0>().as_u256())
    }

    pub async fn record_receipt(&self, indexing: &Indexing, receipt: &[u8], status: ReceiptStatus) {
        let legacy_pool = self.legacy_pools.read().await;
        let mut legacy_pool = match legacy_pool.get(indexing) {
            Some(legacy_pool) => legacy_pool.lock().await,
            None => return,
        };
        legacy_pool.release(receipt, status);
    }

    pub async fn update_allocations(&self, indexings: HashMap<Indexing, Address>) {
        for (indexing, allocation) in indexings {
            let legacy_pool = self.get(&indexing).await;
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
            if !legacy_pool.contains_allocation(&allocation) {
                legacy_pool.add_allocation(self.signer_key, allocation.into());
            }
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
