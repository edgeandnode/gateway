use crate::{indexer_selection::SecretKey, prelude::*};
pub use receipts_allocation::QueryStatus;
use receipts_allocation::ReceiptPool as ReceiptPoolAllocation;
use receipts_transfer::ReceiptPool as ReceiptPoolTransfer;
pub use receipts_transfer::{BorrowFail, ReceiptBorrow};
use std::convert::TryFrom;

#[derive(Default)]
pub struct Receipts {
    transfers: Option<ReceiptPoolTransfer>,
    allocations: Option<ReceiptPoolAllocation>,
}

impl Receipts {
    pub fn has_collateral_for(&self, fee: &GRT) -> bool {
        if let Some(transfer_receipts) = &self.transfers {
            transfer_receipts.has_collateral_for(fee.shift::<0>().as_u256())
        } else if let Some(allocation_receipts) = &self.allocations {
            allocation_receipts.has_collateral_for()
        } else {
            false
        }
    }

    pub fn release(&mut self, receipt: &[u8], status: QueryStatus) {
        match receipt.len() {
            164 => {
                if let Some(allocations) = &mut self.allocations {
                    allocations.release(receipt, status);
                }
            }
            165 => {
                if let Some(transfers) = &mut self.transfers {
                    transfers.release(receipt, status.into());
                }
            }
            _ => panic!("Unrecognized receipt format"),
        }
    }

    pub fn commit(
        &mut self,
        locked_fee: &GRT,
    ) -> Result<receipts_transfer::ReceiptBorrow, BorrowFail> {
        let locked_fee = locked_fee.shift::<0>().as_u256();
        if let Some(transfers) = &mut self.transfers {
            transfers.commit(locked_fee)
        } else if let Some(allocations) = &mut self.allocations {
            let commitment = allocations.commit(locked_fee)?;
            Ok(receipts_transfer::ReceiptBorrow {
                commitment,
                low_collateral_warning: false,
            })
        } else {
            Err(BorrowFail::InsufficientCollateral)
        }
    }

    pub fn add_allocation(&mut self, allocation_id: Address, secret: SecretKey) {
        let allocations = self
            .allocations
            .get_or_insert_with(|| ReceiptPoolAllocation::new());
        allocations.add_allocation(secret, *allocation_id);
    }

    pub fn add_transfer(
        &mut self,
        vector_transfer_id: Bytes32,
        collateral: &GRT,
        secret: SecretKey,
    ) {
        let transfers = self
            .transfers
            .get_or_insert_with(|| ReceiptPoolTransfer::new());
        transfers.add_transfer(
            secret,
            collateral.shift::<0>().as_u256(),
            *vector_transfer_id,
        );
    }

    pub fn remove_allocation(&mut self, allocation_id: &Address) {
        if let Some(allocations) = &mut self.allocations {
            allocations.remove_allocation(allocation_id);
        }
    }

    pub fn remove_transfer(&mut self, vector_transfer_id: &Bytes32) {
        if let Some(transfers) = &mut self.transfers {
            transfers.remove_transfer(vector_transfer_id);
        }
    }

    pub fn recommended_collateral(&self) -> GRT {
        if let Some(transfers) = &self.transfers {
            GRTWei::try_from(transfers.recommended_collateral())
                .unwrap()
                .shift()
        } else {
            GRT::zero()
        }
    }
}
