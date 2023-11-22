use graphql_http::http::request::IntoRequestParameters;
use graphql_http::http_client::{ReqwestExt, ResponseResult};
use serde::de::DeserializeOwned;
use toolshed::url::Url;

/// Send an authenticated GraphQL query to a subgraph.
pub async fn send_query<T>(
    client: &reqwest::Client,
    url: Url,
    ticket: Option<&str>,
    query: impl IntoRequestParameters + Send,
) -> Result<ResponseResult<T>, String>
where
    T: DeserializeOwned,
{
    let mut builder = client.post(url.0);

    if let Some(ticket) = ticket {
        builder = builder.bearer_auth(ticket)
    }

    // TODO: delete before merge
    builder = builder.header("Origin", "https://thegraph.com");

    let res = builder
        .send_graphql(query)
        .await
        .map_err(|err| err.to_string())?;

    Ok(res)
}

pub async fn send_subgraph_query<T>(
    client: &reqwest::Client,
    subgraph_url: Url,
    ticket: Option<&str>,
    query: impl IntoRequestParameters + Send,
) -> Result<T, String>
where
    T: DeserializeOwned,
{
    send_query(client, subgraph_url, ticket, query)
        .await
        .map_err(|err| format!("Error sending subgraph graphql query: {}", err))?
        .map_err(|err| err.to_string())
}

/// Subgraphs sometimes fall behind, be it due to failing or the Graph Node may be having issues. The
/// `_meta` field can now be added to any query so that it is possible to determine against which block
/// the query was effectively executed.
pub mod meta {
    use serde::Deserialize;
    use toolshed::thegraph::BlockPointer;
    use toolshed::url::Url;

    use super::send_query;

    const SUBGRAPH_META_QUERY_DOCUMENT: &str = r#"{ meta: _meta { block { number hash } } }"#;

    #[derive(Debug, Deserialize)]
    pub struct SubgraphMetaQueryResponse {
        pub meta: Meta,
    }

    #[derive(Debug, Deserialize)]
    pub struct Meta {
        pub block: BlockPointer,
    }

    pub async fn send_subgraph_meta_query(
        client: &reqwest::Client,
        subgraph_url: Url,
        ticket: Option<&str>,
    ) -> Result<SubgraphMetaQueryResponse, String> {
        send_query(client, subgraph_url, ticket, SUBGRAPH_META_QUERY_DOCUMENT)
            .await
            .map_err(|err| format!("Error sending subgraph meta query: {}", err))?
            .map_err(|err| err.to_string())
    }
}

pub mod page {
    use alloy_primitives::{BlockHash, BlockNumber};
    use graphql_http::graphql::{Document, IntoDocument, IntoDocumentWithVariables};
    use graphql_http::http_client::ResponseResult;
    use indoc::indoc;
    use serde::{Deserialize, Serialize};
    use serde_json::value::RawValue;
    use toolshed::url::Url;

    use super::{meta::Meta, send_query};

    /// The block at which the query should be executed.
    ///
    /// This is part of the input arguments of the [`SubgraphPageQuery`].
    #[derive(Clone, Debug, Default, Serialize)]
    pub struct BlockHeight {
        /// Value containing a block hash
        #[serde(skip_serializing_if = "Option::is_none")]
        hash: Option<BlockHash>,

        /// Value containing a block number
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<BlockNumber>,

        /// Value containing the minimum block number.
        ///
        /// In the case of `number_gte`, the query will be executed on the latest block only if
        /// the subgraph has progressed to or past the minimum block number.
        /// Defaults to the latest block when omitted.
        #[serde(skip_serializing_if = "Option::is_none")]
        number_gte: Option<BlockNumber>,
    }

    impl BlockHeight {
        pub fn new_with_block_number_gte(number_gte: BlockNumber) -> Self {
            Self {
                number_gte: Some(number_gte),
                ..Default::default()
            }
        }

        pub fn new_with_block_hash(hash: BlockHash) -> Self {
            Self {
                hash: Some(hash),
                ..Default::default()
            }
        }
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct SubgraphPageQueryVars {
        /// The block at which the query should be executed.
        block: BlockHeight,
        first: usize,
        last: String,
    }

    pub struct SubgraphPageQuery {
        query: Document,
        vars: SubgraphPageQueryVars,
    }

    impl SubgraphPageQuery {
        pub fn new(
            query: impl IntoDocument,
            block: BlockHeight,
            first: usize,
            last: String,
        ) -> Self {
            Self {
                query: query.into_document(),
                vars: SubgraphPageQueryVars { block, first, last },
            }
        }
    }

    impl IntoDocumentWithVariables for SubgraphPageQuery {
        type Variables = SubgraphPageQueryVars;

        fn into_document_with_variables(self) -> (Document, Self::Variables) {
            let query = format!(
                indoc! {
                    r#"query ($block: Block_height!, $first: Int!, $last: String!) {{
                    meta: _meta(block: $block) {{ block {{ number hash }} }}
                    results: {query}
                }}"#
                },
                query = self.query
            );

            (query.into_document(), self.vars)
        }
    }

    #[derive(Debug, Deserialize)]
    pub struct SubgraphPageQueryResponse {
        pub meta: Meta,
        pub results: Vec<Box<RawValue>>,
    }

    #[derive(Debug, Deserialize)]
    pub struct SubgraphPageQueryResponseOpaqueEntry {
        pub id: String,
    }

    pub async fn send_subgraph_page_query(
        client: &reqwest::Client,
        subgraph_url: Url,
        ticket: Option<&str>,
        query: impl IntoDocument,
        block_height: BlockHeight,
        batch_size: usize,
        last: Option<String>,
    ) -> Result<ResponseResult<SubgraphPageQueryResponse>, String> {
        send_query(
            client,
            subgraph_url,
            ticket,
            SubgraphPageQuery::new(query, block_height, batch_size, last.unwrap_or_default()),
        )
        .await
        .map_err(|err| format!("Error sending subgraph graphql query: {}", err))
    }
}
