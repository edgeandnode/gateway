use serde::{self, Deserialize, Deserializer, Serialize, Serializer};
pub use std::{fmt, str::FromStr};

macro_rules! bytes_wrapper {
    ($vis:vis, $id:ident, $len:expr) => {
        #[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        $vis struct $id {
            pub bytes: [u8; $len],
        }
        impl From<[u8; $len]> for $id {
            fn from(bytes: [u8; $len]) -> Self {
                Self { bytes }
            }
        }
        impl std::ops::Deref for $id {
            type Target = [u8; $len];
            fn deref(&self) -> &Self::Target {
                &self.bytes
            }
        }
        impl<'de> Deserialize<'de> for $id {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let input: &str = Deserialize::deserialize(deserializer)?;
                input.parse::<Self>().map_err(serde::de::Error::custom)
            }
        }
        impl Serialize for $id {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.to_string())
            }
        }
    };
    ($vis:vis, $id:ident, $len:expr, "HexStr") => {
        bytes_wrapper!($vis, $id, $len);
        impl FromStr for $id {
            type Err = hex::FromHexError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let mut bytes = [0u8; $len];
                let offset = if s.starts_with("0x") {2} else {0};
                hex::decode_to_slice(&s[offset..], &mut bytes)?;
                Ok(Self { bytes })
            }
        }
        impl fmt::Debug for $id {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "0x{}", hex::encode(self.bytes))
            }
        }
        impl fmt::Display for $id {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{:?}", self)
            }
        }
    };
}

bytes_wrapper!(pub, Address, 20, "HexStr");
bytes_wrapper!(pub, Bytes32, 32, "HexStr");
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

impl FromStr for SubgraphDeploymentID {
    type Err = BadIPFSHash;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_ipfs_hash(s).ok_or(BadIPFSHash)
    }
}

impl fmt::Debug for SubgraphDeploymentID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.ipfs_hash())
    }
}

impl fmt::Display for SubgraphDeploymentID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BadIPFSHash;

impl fmt::Display for BadIPFSHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid IPFS hash")
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
