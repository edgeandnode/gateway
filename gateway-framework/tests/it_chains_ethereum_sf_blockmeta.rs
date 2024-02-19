//! Ethereum JSON-RPC API client integration tests

use assert_matches::assert_matches;
use thegraph::types::{BlockHash, BlockNumber};
use tonic::transport::Uri;

use gateway_framework::chains::ethereum::sf_blockmeta_client::SfBlockmetaClient;

/// Test helper to get the test URI as a [`Uri`].
///
/// The URI is expected to be set in the `IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI` environment
/// variable.
fn test_rpc_uri() -> Uri {
    std::env::var("IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI")
        .expect("Missing IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI")
        .parse()
        .expect("Invalid URI")
}

/// Test helper to get the test authorization token.
///
/// The token is expected to be set in the `IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN`
/// environment variable.
fn test_auth_token() -> String {
    let token = std::env::var("IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN")
        .expect("Missing IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN");
    if token.is_empty() {
        panic!("IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN is empty");
    }
    token
}

/// It should be able to retrieve the latest block using the `Head` RPC method.
#[test_with::env(
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI,
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN
)]
#[tokio::test]
async fn fetch_latest_block() {
    //* Given
    let mut client = SfBlockmetaClient::new_with_auth(test_rpc_uri(), test_auth_token());

    //* When
    let resp = client.get_latest_block().await;

    //* Then
    // Assert the block number and hash are present
    assert_matches!(resp, Ok(Some(block)) => {
        assert!(block.num > 0);
        assert!(!block.id.is_empty());
    });
}

/// It should be able to retrieve a block associated with a certain hash using the
/// `IDToNum` RPC method.
#[test_with::env(
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI,
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN
)]
#[tokio::test]
async fn fetch_known_block_by_hash() {
    //* Given
    let mut client = SfBlockmetaClient::new_with_auth(test_rpc_uri(), test_auth_token());

    // The Merge: https://etherscan.io/block/15537394
    let block_hash: BlockHash =
        "0x56a9bb0302da44b8c0b3df540781424684c3af04d0b7a38d72842b762076a664"
            .parse()
            .expect("invalid hash");
    let expected_block_number: u64 = 15_537_394;

    //* When
    let resp = client.get_block_by_hash(block_hash).await;

    //* Then
    // Assert the block number and hash are present
    assert_matches!(resp, Ok(Some(block)) => {
        assert_eq!(block.num, expected_block_number);
        assert_eq!(block.id, format!("{:x}", block_hash));
    });
}

/// It should be able to retrieve a block associated with a certain hash using the
/// `NumToID` RPC method.
#[test_with::env(
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI,
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN
)]
#[tokio::test]
async fn fetch_known_block_by_number() {
    //* Given
    let mut client = SfBlockmetaClient::new_with_auth(test_rpc_uri(), test_auth_token());

    // The Merge: https://etherscan.io/block/15537394
    let block_number: BlockNumber = 15_537_394;
    let expected_block_hash: BlockHash =
        "0x56a9bb0302da44b8c0b3df540781424684c3af04d0b7a38d72842b762076a664"
            .parse()
            .expect("invalid hash");

    //* When
    let resp = client.get_block_by_number(block_number).await;

    //* Then
    // Assert the block number and hash are present
    assert_matches!(resp, Ok(Some(block)) => {
        assert_eq!(block.num, block_number);
        assert_eq!(block.id, format!("{:x}", expected_block_hash));
    });
}

/// It should return `None` when trying to retrieve a block that does not exist using the
/// `IDToNum` RPC method.
#[test_with::env(
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI,
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN
)]
#[tokio::test]
async fn fetch_inexistent_block_by_hash() {
    //* Given
    let mut client = SfBlockmetaClient::new_with_auth(test_rpc_uri(), test_auth_token());

    // Use a hash that does not exist
    let block_hash: BlockHash =
        "0x0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .expect("invalid hash");

    //* When
    let resp = client.get_block_by_hash(block_hash).await;

    //* Then
    // Assert the block number and hash are not present
    assert_matches!(resp, Ok(None));
}

/// It should return `None` when trying to retrieve a block that does not exist using the
/// `NumToID` RPC method.
#[test_with::env(
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_URI,
    IT_GATEWAY_FRAMEWORK_SFBLOCKMETA_AUTH_TOKEN
)]
#[tokio::test]
async fn fetch_inexistent_block_by_number() {
    //* Given
    let mut client = SfBlockmetaClient::new_with_auth(test_rpc_uri(), test_auth_token());

    // Use a very large block number to ensure it does not exist
    let block_number = i64::MAX as BlockNumber;

    //* When
    let resp = client.get_block_by_number(block_number).await;

    //* Then
    // Assert the block number and hash are not present
    assert_matches!(resp, Ok(None));
}
