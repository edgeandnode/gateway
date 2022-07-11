use bs58;
use serde;
use sha3::{digest::Update as _, Digest as _, Keccak256};
pub use std::{fmt, str::FromStr};

macro_rules! bytes_wrapper {
    ($vis:vis, $id:ident, $len:expr) => {
        #[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        $vis struct $id(pub [u8; $len]);

        impl From<[u8; $len]> for $id {
            fn from(bytes: [u8; $len]) -> Self {
                Self(bytes)
            }
        }
        impl std::ops::Deref for $id {
            type Target = [u8; $len];
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl<'de> serde::Deserialize<'de> for $id {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let input: &str = serde::Deserialize::deserialize(deserializer)?;
                input.parse::<Self>().map_err(serde::de::Error::custom)
            }
        }
        impl serde::Serialize for $id {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
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
                Ok(Self(bytes))
            }
        }
        impl fmt::Debug for $id {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "0x{}", hex::encode(self.0))
            }
        }
        impl fmt::Display for $id {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{:?}", self)
            }
        }
    };
}

pub(crate) use bytes_wrapper;

bytes_wrapper!(pub, Address, 20, "HexStr");
bytes_wrapper!(pub, Bytes32, 32, "HexStr");
bytes_wrapper!(pub, SubgraphID, 32);
bytes_wrapper!(pub, SubgraphDeploymentID, 32);

impl FromStr for SubgraphID {
    type Err = InvalidSubgraphID;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_v1(s: &str) -> Option<[u8; 32]> {
            // Attempt to decode v1 format: '0x' <hex account_id> '-' <decimal sequence_id>
            let (account_id, sequence_id) = s.strip_prefix("0x").and_then(|s| s.split_once("-"))?;
            let account = account_id.parse::<Address>().ok()?;
            // Assuming u256 big-endian, since that's the word-size of the EVM
            let mut sequence_word = [0u8; 32];
            let sequence_number = sequence_id.parse::<u64>().ok()?.to_be_bytes();
            sequence_word[24..].copy_from_slice(&sequence_number);
            let hash: [u8; 32] = Keccak256::default()
                .chain(account.as_ref())
                .chain(&sequence_word)
                .finalize()
                .into();
            Some(hash)
        }
        fn parse_v2(s: &str) -> Option<[u8; 32]> {
            // Attempt to decode v2 format: base58 of sha256 hash
            let mut hash = [0u8; 32];
            let len = bs58::decode(s).into(&mut hash).ok()?;
            if len > hash.len() {
                return None;
            }
            hash.rotate_right(32 - len);
            Some(hash)
        }
        if let Some(v2) = parse_v2(s) {
            return Ok(v2.into());
        }
        parse_v1(s).map(Into::into).ok_or(InvalidSubgraphID)
    }
}

impl fmt::Display for SubgraphID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.as_ref()).into_string())
    }
}

impl fmt::Debug for SubgraphID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InvalidSubgraphID;

impl fmt::Display for InvalidSubgraphID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid Subgraph ID")
    }
}

impl SubgraphDeploymentID {
    pub fn from_hex(hex: &str) -> Option<Self> {
        Bytes32::from_str(hex).ok().map(|bytes| Self(bytes.0))
    }

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
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("0x") {
            Self::from_hex(s).ok_or("InvalidSubgraphDeploymentHex")
        } else {
            Self::from_ipfs_hash(s).ok_or("InvalidIPFSHash")
        }
    }
}

impl fmt::Display for SubgraphDeploymentID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.ipfs_hash())
    }
}

impl fmt::Debug for SubgraphDeploymentID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InvalidIPFSHash;

impl fmt::Display for InvalidIPFSHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid IPFS hash")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subgraph_id_encoding() {
        let bytes = hex::decode("67486e65165b1474898247760a4b852d70d95782c6325960e5b6b4fd82fed1bd")
            .unwrap();
        let v1 = "0xdeadbeef678b513255cea949017921c8c9f6ef82-1";
        let v2 = "7xB3yxxD8okmq4dZPky3eP1nYRgLfZrwMyUQBGo32t4U";

        let id1 = v1.parse::<SubgraphID>().unwrap();
        let id2 = v2.parse::<SubgraphID>().unwrap();

        assert_eq!(*id1, bytes.as_slice());
        assert_eq!(&id1.to_string(), v2);
        assert_eq!(*id2, bytes.as_slice());
        assert_eq!(&id2.to_string(), v2);
        assert_eq!(id1, id2);
    }

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
