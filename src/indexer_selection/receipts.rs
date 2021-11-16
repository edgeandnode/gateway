use crate::{indexer_selection::SecretKey, prelude::*};
use lazy_static::lazy_static;
pub use receipts::{
    receipts_to_voucher, BorrowFail, QueryStatus, ReceiptPool, Voucher, VoucherError,
};
use secp256k1::{PublicKey, Secp256k1};
use std::{fmt, ops::Deref};

#[derive(Default)]
pub struct Receipts {
    allocations: ReceiptPool,
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

impl Receipts {
    pub fn has_allocation(&self) -> bool {
        self.allocations.has_collateral_for()
    }

    pub fn release(&mut self, receipt: &[u8], status: QueryStatus) {
        if receipt.len() != 164 {
            panic!("Unrecognized receipt format");
        }
        self.allocations.release(receipt, status);
    }

    pub fn commit(&mut self, locked_fee: &GRT) -> Result<Receipt, BorrowFail> {
        let commitment = self.allocations.commit(locked_fee.shift::<0>().as_u256())?;
        Ok(Receipt { commitment })
    }

    pub fn add_allocation(&mut self, allocation_id: Address, secret: SecretKey) {
        self.allocations.add_allocation(secret, *allocation_id);
    }

    pub fn remove_allocation(&mut self, allocation_id: &Address) {
        self.allocations.remove_allocation(allocation_id);
    }

    pub fn receipts_to_voucher(
        allocation_id: &Address,
        signer: &SecretKey,
        receipts: &[u8],
    ) -> Result<Voucher, VoucherError> {
        lazy_static! {
            static ref SECP256K1: Secp256k1<secp256k1::All> = Secp256k1::new();
        }
        let allocation_signer = PublicKey::from_secret_key(&SECP256K1, signer);
        receipts_to_voucher(allocation_id, &allocation_signer, signer, receipts)
    }
}
