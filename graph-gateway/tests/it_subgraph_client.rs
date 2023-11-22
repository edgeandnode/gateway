use assert_matches::assert_matches;
use serde::Deserialize;
use toolshed::thegraph::{BlockPointer, SubgraphId};
use toolshed::url::Url;

use graph_gateway::subgraph_client::queries::meta::{
    send_subgraph_meta_query, SubgraphMetaQueryResponse,
};
use graph_gateway::subgraph_client::queries::page::{send_subgraph_page_query, BlockHeight};
use graph_gateway::subgraph_client::Client as SubgraphClient;

/// Test helper to parse a URL.
fn test_url(url: &str) -> Url {
    url.parse().expect("Invalid URL")
}

/// Test helper to get the test query key from the environment.
fn test_query_key() -> String {
    std::env::var("THEGRAPH_TEST_QUERY_KEY").expect("Missing THEGRAPH_TEST_QUERY_KEY")
}

#[test_with::env(THEGRAPH_TEST_QUERY_KEY)]
#[tokio::test]
async fn send_subgraph_meta_query_request() {
    //// Given
    let ticket = test_query_key();

    let http_client = reqwest::Client::new();
    let subgraph_url = test_url("https://gateway.thegraph.com/api/deployments/id/QmRbgjyzEgfxGbodu6itfkXCQ5KA9oGxKscrcQ9QuF88oT");

    //// When
    let req_fut = send_subgraph_meta_query(&http_client, subgraph_url, Some(&ticket));
    let res = tokio::time::timeout(std::time::Duration::from_secs(10), req_fut)
        .await
        .expect("Timeout on subgraph meta query");

    //// Then
    // Assert the query succeeded and we get a non-empty block number and hash.
    assert_matches!(res, Ok(SubgraphMetaQueryResponse { meta }) => {
        assert!(meta.block.number > 0);
        assert!(!meta.block.hash.is_empty());
    });
}

#[test_with::env(THEGRAPH_TEST_QUERY_KEY)]
#[tokio::test]
async fn send_subgraph_page_query_request() {
    //// Given
    const PAGE_REQUEST_BATCH_SIZE: usize = 6;

    let ticket = test_query_key();

    let http_client = reqwest::Client::new();
    let subgraph_url = test_url("https://gateway.thegraph.com/api/deployments/id/QmRbgjyzEgfxGbodu6itfkXCQ5KA9oGxKscrcQ9QuF88oT");

    // Query all subgraph ids.
    const SUBGRAPHS_QUERY_DOCUMENT: &str = r#"
        subgraphs(
            block: $block
            orderBy: id, orderDirection: asc
            first: $first
            where: {
                id_gt: $last
                entityVersion: 2
            }
        ) {
            id
        }
        "#;

    //// When
    let req_fut = send_subgraph_page_query(
        &http_client,
        subgraph_url,
        Some(&ticket),
        SUBGRAPHS_QUERY_DOCUMENT,
        BlockHeight::new_with_block_number_gte(18627000),
        PAGE_REQUEST_BATCH_SIZE,
        None,
    );
    let res = tokio::time::timeout(std::time::Duration::from_secs(10), req_fut)
        .await
        .expect("Timeout on subgraph meta query");

    //// Then
    assert_matches!(res, Ok(Ok(resp)) => {
        // Assert meta data is present and valid.
        assert!(resp.meta.block.number > 0);
        assert!(!resp.meta.block.hash.is_empty());

        // Assert the results are present and the correct size.
        assert_eq!(resp.results.len(), PAGE_REQUEST_BATCH_SIZE);
    });
}

#[test_with::env(THEGRAPH_TEST_QUERY_KEY)]
#[tokio::test]
async fn client_send_query() {
    //// Given
    let ticket = test_query_key();

    let http_client = reqwest::Client::new();
    let subgraph_url = test_url("https://gateway.thegraph.com/api/deployments/id/QmRbgjyzEgfxGbodu6itfkXCQ5KA9oGxKscrcQ9QuF88oT");

    let client = SubgraphClient::builder(http_client, subgraph_url)
        .with_auth_token(Some(ticket))
        .build();

    // Subgraph meta query
    const SUBGRAPH_META_QUERY_DOCUMENT: &str = r#"{ meta: _meta { block { number hash } } }"#;

    #[derive(Debug, Deserialize)]
    struct Meta {
        block: BlockPointer,
    }

    #[derive(Debug, Deserialize)]
    struct SubgraphMetaQueryResponse {
        meta: Meta,
    }

    //// When
    let req_fut = client.query::<SubgraphMetaQueryResponse>(SUBGRAPH_META_QUERY_DOCUMENT);
    let res = tokio::time::timeout(std::time::Duration::from_secs(10), req_fut)
        .await
        .expect("Timeout on subgraph meta query");

    //// Then
    // Assert the query succeeded and we get a non-empty block number and hash.
    assert_matches!(res, Ok(SubgraphMetaQueryResponse { meta }) => {
        assert!(meta.block.number > 0);
        assert!(!meta.block.hash.is_empty());
    });
}

#[test_with::env(THEGRAPH_TEST_QUERY_KEY)]
#[tokio::test]
async fn send_subgraph_paginated() {
    //// Given
    let ticket = test_query_key();
    let subgraph_url = test_url("https://gateway.thegraph.com/api/deployments/id/QmRbgjyzEgfxGbodu6itfkXCQ5KA9oGxKscrcQ9QuF88oT");

    let http_client = reqwest::Client::new();

    let mut client = SubgraphClient::builder(http_client, subgraph_url)
        .with_auth_token(Some(ticket))
        .build();

    // Query all subgraph ids.
    const SUBGRAPHS_QUERY_DOCUMENT: &str = r#"
        subgraphs(
            block: $block
            orderBy: id, orderDirection: asc
            first: $first
            where: {
                id_gt: $last
                entityVersion: 2
            }
        ) {
            id
        }
        "#;

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Subgraph {
        pub id: SubgraphId,
    }

    //// When
    let req_fut = client.paginated_query::<Subgraph>(SUBGRAPHS_QUERY_DOCUMENT);
    let res = tokio::time::timeout(std::time::Duration::from_secs(10), req_fut)
        .await
        .expect("Timeout on subgraph paginated query");

    //// Then
    // Assert the query succeeded and we got a non-empty list of active subscriptions.
    assert_matches!(res, Ok(vec) => {
        assert!(!vec.is_empty());
    });
}
