use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

pub use indexer_selection::receipts::QueryStatus as ReceiptStatus;
use indexer_selection::{
    receipts::{BorrowFail, ReceiptPool},
    Indexing, SecretKey,
};
use prelude::{
    tokio::sync::{Mutex, RwLock},
    *,
};

#[derive(Default)]
pub struct ReceiptPools {
    pools: RwLock<HashMap<Indexing, Arc<Mutex<ReceiptPool>>>>,
}

impl ReceiptPools {
    async fn get(&self, indexing: &Indexing) -> Arc<Mutex<ReceiptPool>> {
        if let Some(pool) = self.pools.read().await.get(indexing) {
            return pool.clone();
        }
        let mut pools = self.pools.write().await;
        match pools.entry(*indexing) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let pool = Arc::new(Mutex::default());
                entry.insert(pool.clone());
                pool
            }
        }
    }

    pub async fn commit(&self, indexing: &Indexing, fee: GRT) -> Result<Vec<u8>, BorrowFail> {
        let pool = self
            .pools
            .read()
            .await
            .get(indexing)
            .cloned()
            .ok_or(BorrowFail::NoAllocation)?;
        let mut pool = pool.lock().await;
        pool.commit(fee.shift::<0>().as_u256())
    }

    pub async fn release(&self, indexing: &Indexing, receipt: &[u8], status: ReceiptStatus) {
        if receipt.len() != 164 {
            panic!("Unrecognized receipt format");
        }
        let pool = self.pools.read().await;
        let mut pool = match pool.get(indexing) {
            Some(pool) => pool.lock().await,
            None => return,
        };
        pool.release(receipt, status);
    }

    pub async fn update_receipt_pool(
        &self,
        signer: &SecretKey,
        indexing: &Indexing,
        new_allocations: &HashMap<Address, GRT>,
    ) {
        let pool = self.get(indexing).await;
        let mut pool = pool.lock().await;
        // Remove allocations not present in new_allocations
        for old_allocation in pool.addresses() {
            if new_allocations
                .iter()
                .all(|(id, _)| old_allocation != id.as_ref())
            {
                pool.remove_allocation(&old_allocation);
            }
        }
        // Add new_allocations not present in allocations
        for id in new_allocations.keys() {
            if !pool.contains_allocation(id) {
                pool.add_allocation(*signer, id.0);
            }
        }
    }
}
