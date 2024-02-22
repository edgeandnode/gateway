use std::fmt::Display;
use std::{fmt, ops::Deref, str::FromStr};

use alloy_primitives::B256;
use custom_debug::CustomDebug;
use secp256k1::SecretKey;
use serde::Deserialize;
use serde_with::{serde_as, DeserializeAs, DisplayFromStr};
use url::Url;

// TODO: Move this to the graph-gateway::config module
#[serde_as]
#[derive(Clone, CustomDebug, Deserialize)]
pub struct Chain {
    /// The first name is used in logs, the others are aliases also supported in subgraph manifests.
    pub names: Vec<String>,
    #[serde_as(as = "DisplayFromStr")]
    #[debug(with = "Display::fmt")]
    pub rpc: Url,
    #[debug(skip)]
    pub rpc_auth: Option<String>,
}

// TODO: Move this to the graph-gateway::config module
impl From<Chain> for crate::chains::ethereum::Config {
    fn from(value: Chain) -> Self {
        Self {
            names: value.names,
            url: value.rpc,
        }
    }
}

// TODO: Move this to the graph-gateway::config module
impl From<Chain> for crate::chains::blockmeta::Config {
    fn from(value: Chain) -> Self {
        Self {
            names: value.names,
            uri: value.rpc.as_str().parse().expect("invalid uri"),
            auth: value.rpc_auth.expect("missing rpc auth"),
        }
    }
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
