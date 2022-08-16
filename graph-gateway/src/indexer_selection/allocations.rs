use crate::indexer_selection::SecretKey;
use prelude::*;
pub use receipts::{BorrowFail, PartialVoucher, QueryStatus, ReceiptPool, Voucher, VoucherError};
use std::{collections::HashMap, fmt, ops::Deref};

#[derive(Default)]
pub struct Allocations {
    receipts: ReceiptPool,
    pub allocations: HashMap<Address, GRT>,
}

#[derive(Clone)]
pub struct Receipt {
    pub commitment: Vec<u8>,
}

impl From<Vec<u8>> for Receipt {
    fn from(commitment: Vec<u8>) -> Self {
        Self { commitment }
    }
}

impl Deref for Receipt {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.commitment
    }
}

impl fmt::Debug for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "0x{}", hex::encode(&self.commitment))
    }
}

impl Allocations {
    pub fn contains_allocation(&self, allocation_id: &Address) -> bool {
        self.receipts.contains_allocation(allocation_id)
    }

    pub fn allocation_ids(&self) -> Vec<Address> {
        self.receipts
            .addresses()
            .into_iter()
            .map::<Address, _>(Into::into)
            .collect()
    }

    pub fn add_allocation(&mut self, allocation_id: Address, secret: SecretKey, size: GRT) {
        self.receipts.add_allocation(secret, *allocation_id);
        self.allocations.insert(allocation_id, size);
    }

    pub fn remove_allocation(&mut self, allocation_id: &Address) {
        self.receipts.remove_allocation(allocation_id);
        self.allocations.remove(allocation_id);
    }

    pub fn total_allocation(&self) -> GRT {
        self.allocations
            .iter()
            .map(|(_, size)| size)
            .fold(GRT::zero(), |sum, size| sum + *size)
    }

    pub fn release(&mut self, receipt: &[u8], status: QueryStatus) {
        if receipt.len() != 164 {
            panic!("Unrecognized receipt format");
        }
        self.receipts.release(receipt, status);
    }

    pub fn commit(&mut self, locked_fee: &GRT) -> Result<Receipt, BorrowFail> {
        let commitment = self.receipts.commit(locked_fee.shift::<0>().as_u256())?;
        Ok(Receipt { commitment })
    }
}
