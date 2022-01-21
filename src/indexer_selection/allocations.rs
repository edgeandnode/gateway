use crate::{indexer_selection::SecretKey, prelude::*};
pub use receipts::{BorrowFail, PartialVoucher, QueryStatus, ReceiptPool, Voucher, VoucherError};
use std::{fmt, ops::Deref};

#[derive(Default)]
pub struct Allocations {
    receipts: ReceiptPool,
    pub total_allocation: GRT,
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

    pub fn add_allocation(&mut self, allocation_id: Address, secret: SecretKey, size: GRT) {
        self.receipts.add_allocation(secret, *allocation_id);
        self.total_allocation = self.total_allocation.saturating_add(size);
    }

    pub fn remove_allocation(&mut self, allocation_id: &Address, size: GRT) {
        self.receipts.remove_allocation(allocation_id);
        self.total_allocation = self.total_allocation.saturating_sub(size);
    }
}
