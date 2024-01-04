use std::{fmt, ops::Deref, str::FromStr};

use alloy_primitives::B256;
use secp256k1::SecretKey;
use serde::Deserialize;
use serde_with::{serde_as, DeserializeAs, DisplayFromStr};
use toolshed::url::Url;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct Chain {
    /// The first name is used in logs, the others are aliases also supported in subgraph manifests.
    pub names: Vec<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub rpc: Url,
}

#[derive(Deserialize)]
#[serde(transparent)]
pub struct Hidden<T>(pub T);

impl<T: fmt::Debug> fmt::Debug for Hidden<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HIDDEN")
    }
}

impl<T: FromStr> FromStr for Hidden<T> {
    type Err = T::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl<T> Deref for Hidden<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct HiddenSecretKey;

impl<'de> DeserializeAs<'de, Hidden<SecretKey>> for HiddenSecretKey {
    fn deserialize_as<D>(deserializer: D) -> Result<Hidden<SecretKey>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = B256::deserialize(deserializer)?;
        SecretKey::from_slice(bytes.as_slice())
            .map(Hidden)
            .map_err(serde::de::Error::custom)
    }
}
