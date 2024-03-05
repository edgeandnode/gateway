//! Ethereum JSON-RPC API client.

use alloy_primitives::BlockHash;
use serde::de::DeserializeOwned;
use serde_json::json;
use serde_json::Value as Json;
use url::Url;

use super::json_rpc;

pub use self::rpc_api_types::Block;
pub use self::rpc_api_types::BlockByNumberParam;

/// Ethereum JSON-RPC API types.
mod rpc_api_types {
    use alloy_primitives::{BlockHash, BlockNumber};
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serialize};

    /// A block object.
    ///
    /// See the [eth_getBlockByHash](https://ethereum.org/en/developers/docs/apis/json-rpc#eth_getblockbyhash)
    /// returned object.
    #[derive(Debug, Deserialize)]
    pub struct Block {
        /// The block hash. `None` when its pending block.
        pub hash: Option<BlockHash>,

        /// The block number. `None` when its pending block.
        #[serde(deserialize_with = "deserialize_option_u64")]
        pub number: Option<BlockNumber>,

        /// Array of uncle hashes.
        #[serde(default)]
        pub uncles: Vec<BlockHash>,
    }

    impl Block {
        /// Returns `true` if the block is pending.
        pub fn is_pending(&self) -> bool {
            self.hash.is_none() || self.number.is_none()
        }
    }

    /// Deserialize an `u64` number from a "nullable" hex string.
    ///
    /// From [JSON-RPC API Conventions - Quantities](https://ethereum.org/en/developers/docs/apis/json-rpc#quantities-encoding):
    ///
    /// > When encoding quantities (integers, numbers): encode as hex, prefix with "0x", the most
    /// > compact representation (slight exception: zero should be represented as "0x0").
    fn deserialize_option_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input: Option<&str> = Deserialize::deserialize(deserializer)?;
        match input {
            None => Ok(None),
            Some(input) => {
                // The input should start with "0x".
                if !input.starts_with("0x") {
                    return Err(Error::custom("invalid quantity format"));
                }

                let quantity = u64::from_str_radix(&input[2..], 16).map_err(Error::custom)?;
                Ok(Some(quantity))
            }
        }
    }

    /// The parameters for the `eth_getBlockByNumber` method.
    ///
    /// See: [eth_getBlockByNumber](https://ethereum.org/en/developers/docs/apis/json-rpc#eth_getblockbynumber)
    #[derive(Debug, Clone, Copy)]
    pub enum BlockByNumberParam {
        /// Get the earliest/genesis block.
        Earliest,
        /// Get the latest mined block.
        Latest,
        /// Get the pending block (if any).
        Pending,
        /// Get the block with the given number.
        Number(BlockNumber),
    }

    impl Serialize for BlockByNumberParam {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::ser::Serializer,
        {
            match self {
                Self::Earliest => "earliest".serialize(serializer),
                Self::Latest => "latest".serialize(serializer),
                Self::Pending => "pending".serialize(serializer),
                Self::Number(number) => format!("{:#x}", number).serialize(serializer),
            }
        }
    }
}

/// A client for interacting with an Ethereum JSON-RPC server API.
///
/// This client does not aim to be a full-featured Ethereum RPC client,
/// but rather a simple client for fetching block data from an Ethereum
/// node.
#[derive(Clone)]
pub struct EthRpcClient {
    http_client: reqwest::Client,
    url: Url,
}

impl EthRpcClient {
    /// Create a new client for the given Ethereum JSON-RPC server.
    pub fn new(http_client: reqwest::Client, url: Url) -> Self {
        Self { http_client, url }
    }

    /// Get the current client version.
    ///
    /// See: [web3_clientVersion](https://ethereum.org/en/developers/docs/apis/json-rpc#web3_clientversion)
    pub async fn get_client_version(&self) -> anyhow::Result<String> {
        match self.send_request("web3_clientVersion", json!([])).await {
            Ok(Some(version)) => Ok(version),
            Ok(None) => Err(anyhow::anyhow!("unknown client version")),
            Err(err) => Err(err),
        }
    }

    /// Fetch the block with the given hash from the Ethereum node.
    ///
    /// - `hash`: The block hash to fetch.
    ///
    /// See: [eth_getBlockByHash](https://ethereum.org/en/developers/docs/apis/json-rpc#eth_getblockbyhash)
    pub async fn get_block_by_hash(&self, hash: BlockHash) -> anyhow::Result<Option<Block>> {
        self.send_request("eth_getBlockByHash", json!([hash.to_string(), false]))
            .await
    }

    /// Fetch the block with the given number from the Ethereum node.
    ///
    /// - `by`: The block number to fetch. See [`BlockByNumberParam`] for more details.
    ///
    /// See: [eth_getBlockByNumber](https://ethereum.org/en/developers/docs/apis/json-rpc#eth_getblockbynumber)
    pub async fn get_block_by_number(
        &self,
        by: BlockByNumberParam,
    ) -> anyhow::Result<Option<Block>> {
        self.send_request("eth_getBlockByNumber", json!([by, false]))
            .await
    }

    /// Send a raw JSON-RPC request to the Ethereum node using the given method and parameters.
    pub async fn send_request<T: DeserializeOwned>(
        &self,
        method: &'static str,
        params: Json,
    ) -> anyhow::Result<Option<T>> {
        let request_id = json_rpc::new_request_id();

        let request = json_rpc::Request::new(&request_id, method, params);
        json_rpc::send_request(&self.http_client, &self.url, &request).await
    }
}
