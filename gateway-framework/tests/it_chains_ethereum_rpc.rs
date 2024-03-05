//! Ethereum JSON-RPC API client integration tests

use std::str::FromStr;

use alloy_primitives::{BlockHash, BlockNumber};
use assert_matches::assert_matches;
use url::Url;

use gateway_framework::chains::ethereum::rpc_client::{BlockByNumberParam, EthRpcClient};

// Use the publicnode Ethereum RPC URL for testing as it is a Geth-based public and
// non-authenticated RPC endpoint
const TEST_RPC_URL: &str = "https://ethereum.publicnode.com";

/// Helper function to get the test Ethereum RPC URL as a [`Url`].
fn test_rpc_url() -> Url {
    TEST_RPC_URL.parse().expect("Failed to parse URL")
}

/// It should be able to retrieve the ehtereum client version using the `web3_clientVersion` RPC
/// method.
#[tokio::test]
async fn eth_client_fetch_client_version() {
    //* Given
    let client = EthRpcClient::new(reqwest::Client::new(), test_rpc_url());

    //* When
    let resp = client.get_client_version().await;

    //* Then
    // Assert the client version is present
    assert_matches!(resp, Ok(version) => assert!(!version.is_empty()));
}

/// It should be able to retrieve the latest block using the `eth_getBlockByNumber` RPC method.
#[tokio::test]
async fn fetch_latest_block() {
    //* Given
    let client = EthRpcClient::new(reqwest::Client::new(), test_rpc_url());

    //* When
    let resp = client.get_block_by_number(BlockByNumberParam::Latest).await;

    //* Then
    // Assert the block number and hash are present
    assert_matches!(resp, Ok(Some(block)) => {
        assert_matches!(block.number, Some(num) => assert!(num > 0));
        assert_matches!(block.hash, Some(hash) => assert!(!hash.is_empty()));
    });
}

/// It should be able to retrieve a block associated with a certain hash using the
/// `eth_getBlockByHash` RPC method.
#[tokio::test]
async fn fetch_known_block_by_hash() {
    //* Given
    let client = EthRpcClient::new(reqwest::Client::new(), test_rpc_url());

    // The Merge: https://etherscan.io/block/15537394
    let block_hash =
        BlockHash::from_str("0x56a9bb0302da44b8c0b3df540781424684c3af04d0b7a38d72842b762076a664")
            .expect("invalid hash");
    let expected_block_number: BlockNumber = 15_537_394;

    //* When
    let resp = client.get_block_by_hash(block_hash).await;

    //* Then
    // Assert the block number and hash are present
    assert_matches!(resp, Ok(Some(block)) => {
        assert_matches!(block.number, Some(num) => assert_eq!(num, expected_block_number));
        assert_matches!(block.hash, Some(hash) => assert_eq!(hash, block_hash));
    });
}

/// It should be able to retrieve a block associated with a certain hash using the
/// `eth_getBlockByNumber` RPC method.
#[tokio::test]
async fn fetch_known_block_by_number() {
    //* Given
    let client = EthRpcClient::new(reqwest::Client::new(), test_rpc_url());

    // The Merge: https://etherscan.io/block/15537394
    let block_number: BlockNumber = 15_537_394;
    let expected_block_hash =
        BlockHash::from_str("0x56a9bb0302da44b8c0b3df540781424684c3af04d0b7a38d72842b762076a664")
            .expect("invalid hash");

    //* When
    let resp = client
        .get_block_by_number(BlockByNumberParam::Number(block_number))
        .await;

    //* Then
    // Assert the block number and hash are present
    assert_matches!(resp, Ok(Some(block)) => {
        assert_matches!(block.number, Some(num) => assert_eq!(num, block_number));
        assert_matches!(block.hash, Some(hash) => assert_eq!(hash, expected_block_hash));
    });
}

/// It should return `None` when trying to retrieve a block that does not exist using the
/// `eth_getBlockByHash` RPC method.
#[tokio::test]
async fn fetch_inexistent_block_by_hash() {
    //* Given
    let client = EthRpcClient::new(reqwest::Client::new(), test_rpc_url());

    // Use a hash that does not exist
    let block_hash =
        BlockHash::from_str("0x0000000000000000000000000000000000000000000000000000000000000000")
            .expect("invalid hash");

    //* When
    let resp = client.get_block_by_hash(block_hash).await;

    //* Then
    // Assert the response is `None`
    assert_matches!(resp, Ok(None));
}

/// It should return `None` when trying to retrieve a block that does not exist using the
/// `eth_getBlockByNumber` RPC method.
#[tokio::test]
async fn fetch_inexistent_block_by_number() {
    //* Given
    let client = EthRpcClient::new(reqwest::Client::new(), test_rpc_url());

    // Use a very large block number to ensure it does not exist
    let block_number = i64::MAX as BlockNumber;

    //* When
    let resp = client
        .get_block_by_number(BlockByNumberParam::Number(block_number))
        .await;

    //* Then
    // Assert the response is `None`
    assert_matches!(resp, Ok(None));
}
