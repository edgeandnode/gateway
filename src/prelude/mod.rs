pub mod decimal;
pub mod shared_lookup;
pub mod weighted_sample;

#[cfg(test)]
pub mod test_utils;

pub use crate::prelude::decimal::*;
pub use eventuals::{Eventual, EventualWriter, Ptr};
pub use std::{convert::TryInto, str::FromStr};
pub use tracing;

pub fn init_tracing(json: bool) {
    let logger = tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env());
    if json {
        tracing::subscriber::set_global_default(logger.json().finish())
            .expect("Failed to set global default for tracing");
    } else {
        tracing::subscriber::set_global_default(logger.finish())
            .expect("Failed to set global default for tracing");
    };
}

/// Decimal Parts-Per-Million with 6 fractional digits
pub type PPM = UDecimal<6>;
/// Decimal USD with 18 fractional digits
pub type USD = UDecimal<18>;
/// Decimal GRT with 18 fractional digits
pub type GRT = UDecimal<18>;
/// Decimal GRT Wei (10^-18 GRT)
pub type GRTWei = UDecimal<0>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockPointer {
    pub number: u64,
    pub hash: Bytes32,
}

pub trait Reader {
    type Writer;
    fn new() -> (Self::Writer, Self);
}

macro_rules! bytes_wrapper {
    ($vis:vis, $id:ident, $len:expr) => {
        #[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd, tree_buf::Decode, tree_buf::Encode)]
        $vis struct $id {
            pub bytes: [u8; $len],
        }
        impl From<[u8; $len]> for $id {
            fn from(bytes: [u8; $len]) -> Self {
                Self { bytes }
            }
        }
        impl FromStr for $id {
            type Err = hex::FromHexError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let mut bytes = [0u8; $len];
                let offset = if s.starts_with("0x") {2} else {0};
                hex::decode_to_slice(s.split_at(offset).1, &mut bytes)?;
                Ok(Self { bytes })
            }
        }
        impl std::ops::Deref for $id {
            type Target = [u8; $len];
            fn deref(&self) -> &Self::Target {
                &self.bytes
            }
        }
    };
    ($vis:vis, $id:ident, $len:expr, "HexDebug") => {
        bytes_wrapper!($vis, $id, $len);
        impl std::fmt::Debug for $id {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "0x{}", hex::encode(self.bytes))
            }
        }
    };
}

bytes_wrapper!(pub, Address, 20, "HexDebug");
bytes_wrapper!(pub, Bytes32, 32, "HexDebug");
bytes_wrapper!(pub, SubgraphDeploymentID, 32);

impl SubgraphDeploymentID {
    pub fn from_ipfs_hash(hash: &str) -> Option<Self> {
        let mut decoded = [0u8; 34];
        bs58::decode(hash).into(&mut decoded).ok()?;
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&decoded[2..]);
        Some(bytes.into())
    }

    pub fn ipfs_hash(&self) -> String {
        bs58::encode([&[0x12, 0x20], self.as_ref()].concat()).into_string()
    }
}

impl std::fmt::Debug for SubgraphDeploymentID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.ipfs_hash())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subgraph_deployment_id_encoding() {
        let ipfs_hash = "QmWmyoMoctfbAaiEs2G46gpeUmhqFRDW6KWo64y5r581Vz";
        let mut bytes = [0u8; 32];
        bytes.clone_from_slice(
            &hex::decode("12207d5a99f603f231d53a4f39d1521f98d2e8bb279cf29bebfd0687dc98458e7f89")
                .unwrap()
                .as_slice()[2..],
        );

        let id1 = SubgraphDeploymentID::from(bytes);
        let id2 = SubgraphDeploymentID::from_ipfs_hash(ipfs_hash)
            .expect("Unable to create SubgraphDeploymentID from IPFS hash");

        assert_eq!(id1.ipfs_hash(), ipfs_hash);
        assert_eq!(*id1, bytes);
        assert_eq!(id2.ipfs_hash(), ipfs_hash);
        assert_eq!(*id2, bytes);
        assert_eq!(id1, id2);
    }
}
