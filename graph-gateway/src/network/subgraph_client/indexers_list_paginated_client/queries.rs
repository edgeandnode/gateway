use serde::{de::Error as _, Deserialize};
use thegraph_core::types::Attestation;
use thegraph_graphql_http::{
    http::{
        request::IntoRequestParameters,
        response::{Error, ResponseBody},
    },
    http_client::{RequestError, ResponseError, ResponseResult},
};
use url::Url;

/// Send an authenticated GraphQL query to a subgraph.
pub async fn send_query<T>(
    client: &reqwest::Client,
    url: Url,
    auth: &str,
    query: impl IntoRequestParameters + Send,
) -> anyhow::Result<ResponseResult<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let resp = client
        .post(url)
        .bearer_auth(auth)
        .json(&query.into_request_parameters())
        .send()
        .await?;

    process_indexer_graphql_response(resp)
        .await
        .map_err(anyhow::Error::from)
}

/// The payload of an indexer response.
#[derive(Debug, Deserialize)]
pub struct IndexerResponsePayload {
    /// Encapsulates the GraphQL response data.
    #[serde(rename = "graphQLResponse")]
    pub data: Option<String>,
    /// Response attestation.
    pub attestation: Option<Attestation>,
    /// Error message.
    pub error: Option<String>,
}

async fn process_indexer_graphql_response<ResponseData>(
    resp: reqwest::Response,
) -> Result<ResponseResult<ResponseData>, RequestError>
where
    ResponseData: serde::de::DeserializeOwned,
{
    let status = resp.status();

    // [6.4.1 application/json](https://graphql.github.io/graphql-over-http/draft/#sec-application-json)
    //
    // > The server SHOULD use the 200 status code for every response to a well-formed
    // > GraphQL-over-HTTP request, independent of any GraphQL request error or GraphQL field error
    // > raised.
    //
    // > For compatibility with legacy servers, this specification allows the use of `4xx` or `5xx`
    // > status codes for a failed well-formed GraphQL-over-HTTP request where the response uses
    // > the `application/json` media type, but it is **strongly discouraged**.
    if !status.is_success() && !status.is_client_error() && !status.is_server_error() {
        return Err(RequestError::ResponseRecvError(status, resp.text().await?));
    }

    // Receive the response body.
    let response = resp.bytes().await.map_err(|err| {
        RequestError::ResponseRecvError(status, format!("Error reading response body: {}", err))
    })?;

    // Deserialize the response body.
    let resp: IndexerResponsePayload = serde_json::from_slice(&response).map_err(|error| {
        RequestError::ResponseDeserializationError {
            error,
            response: String::from_utf8_lossy(&response).to_string(),
        }
    })?;

    Ok(match (resp.data, resp.attestation, resp.error) {
        (Some(resp), _, None) => {
            // Deserialize the encapsulated response from the `graphQLResponse` field
            let response: ResponseBody<ResponseData> =
                serde_json::from_str(&resp).map_err(|error| {
                    RequestError::ResponseDeserializationError {
                        error,
                        response: resp.to_string(),
                    }
                })?;

            // Check if the encapsulated response contains errors
            if !response.errors.is_empty() {
                return Err(RequestError::ResponseDeserializationError {
                    error: serde_json::Error::custom("response contains errors"),
                    response: resp.to_string(),
                });
            }

            // Extract the `data` field from the response
            let data = response
                .data
                .ok_or_else(|| RequestError::ResponseDeserializationError {
                    error: serde_json::Error::custom("response data missing"),
                    response: resp.to_string(),
                })?;

            Ok(data)
        }

        // If error is present, return it as a failure
        // Do not consider partial responses
        (_, _, Some(error)) => Err(ResponseError::Failure {
            errors: vec![Error {
                message: error,
                locations: vec![],
                path: vec![],
            }],
        }),

        // Error is required for failed responses
        (None, _, _) => Err(ResponseError::Empty),
    })
}

/// Subgraphs sometimes fall behind, be it due to failing or the Graph Node may be having issues.
/// The `_meta` field can now be added to any query so that it is possible to determine against
/// which block the query was effectively executed.
pub mod meta {
    use serde::Deserialize;
    use thegraph_core::types::BlockPointer;
    use url::Url;

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

    pub async fn send_bootstrap_meta_query(
        client: &reqwest::Client,
        subgraph_url: &Url,
        auth: &str,
    ) -> Result<SubgraphMetaQueryResponse, String> {
        send_query(
            client,
            subgraph_url.clone(),
            auth,
            SUBGRAPH_META_QUERY_DOCUMENT,
        )
        .await
        .map_err(|err| format!("Error sending subgraph meta query: {}", err))?
        .map_err(|err| err.to_string())
    }
}

pub mod page {
    use alloy_primitives::{BlockHash, BlockNumber};
    use serde::{ser::SerializeMap as _, Deserialize, Serialize, Serializer};
    use serde_json::value::RawValue;
    use thegraph_graphql_http::{
        graphql::{Document, IntoDocument, IntoDocumentWithVariables},
        http_client::ResponseResult,
    };
    use url::Url;

    use super::{meta::Meta, send_query};

    /// The block at which the query should be executed.
    ///
    /// This is part of the input arguments of the [`SubgraphPageQuery`].
    #[derive(Clone, Debug, Default)]
    pub enum BlockHeight {
        #[default]
        Latest,
        Hash(BlockHash),
        NumberGte(BlockNumber),
    }

    impl Serialize for BlockHeight {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            let mut obj = s.serialize_map(Some(1))?;
            match self {
                Self::Latest => (),
                Self::Hash(hash) => obj.serialize_entry("hash", hash)?,
                Self::NumberGte(number) => obj.serialize_entry("number_gte", number)?,
            }
            obj.end()
        }
    }

    /// The arguments of the [`SubgraphPageQuery`] query.
    #[derive(Clone, Debug, Serialize)]
    pub struct SubgraphPageQueryVars {
        /// The block at which the query should be executed.
        block: BlockHeight,
        /// The maximum number of entities to fetch.
        first: usize,
        /// The ID of the last entity fetched.
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
            let query = indoc::formatdoc! {
                r#"query ($block: Block_height!, $first: Int!, $last: String!) {{
                    meta: _meta(block: $block) {{ block {{ number hash }} }}
                    results: {query}
                }}"#,
                query = self.query
            };

            (query.into_document(), self.vars)
        }
    }

    #[derive(Debug, Deserialize)]
    pub struct SubgraphPageQueryResponse {
        pub meta: Meta,
        pub results: Vec<Box<RawValue>>,
    }

    /// An opaque entry in the response of a subgraph page query.
    ///
    /// This is used to determine the ID of the last entity fetched.
    #[derive(Debug, Deserialize)]
    pub struct SubgraphPageQueryResponseOpaqueEntry {
        pub id: String,
    }

    pub async fn send_subgraph_page_query(
        client: &reqwest::Client,
        subgraph_url: Url,
        auth: &str,
        query: impl IntoDocument,
        block_height: BlockHeight,
        batch_size: usize,
        last: Option<String>,
    ) -> Result<ResponseResult<SubgraphPageQueryResponse>, String> {
        send_query(
            client,
            subgraph_url,
            auth,
            SubgraphPageQuery::new(query, block_height, batch_size, last.unwrap_or_default()),
        )
        .await
        .map_err(|err| format!("Error sending subgraph graphql query: {}", err))
    }
}
