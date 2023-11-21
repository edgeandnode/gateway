use alloy_primitives::{BlockHash, BlockNumber};
use graphql_http::graphql::{Document, IntoDocument, IntoDocumentWithVariables};
use graphql_http::http::request::IntoRequestParameters;
use graphql_http::http_client::{ReqwestExt, ResponseError, ResponseResult};
use indoc::indoc;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::RawValue;
use toolshed::thegraph::BlockPointer;
use toolshed::url::Url;

// graph-node is rejecting values of `number_gte:0` on subgraphs with a larger `startBlock`
// TODO: delete when resolved
const SUBGRAPH_INIT_QUERY_DOCUMENT: &str = r#"{ meta: _meta { block { number hash } } }"#;

#[derive(Deserialize)]
struct Meta {
    block: BlockPointer,
}

/// The block at which the query should be executed.
///
/// This is part of the input arguments of the [`SubgraphPaginatedQuery`].
#[derive(Clone, Debug, Default, Serialize)]
struct BlockHeight {
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

// graph-node is rejecting values of `number_gte:0` on subgraphs with a larger `startBlock`
// TODO: delete when resolved
#[derive(Deserialize)]
struct SubgraphInitQueryResponse {
    meta: Meta,
}

struct SubgraphPaginatedQuery {
    query: Document,
    vars: SubgraphPaginatedQueryVars,
}

#[derive(Clone, Debug, Serialize)]
struct SubgraphPaginatedQueryVars {
    block: BlockHeight,
    first: u32,
    last: String,
}

impl SubgraphPaginatedQuery {
    fn new(query: impl IntoDocument, vars: SubgraphPaginatedQueryVars) -> Self {
        Self {
            query: query.into_document(),
            vars,
        }
    }
}

impl IntoDocumentWithVariables for SubgraphPaginatedQuery {
    type Variables = SubgraphPaginatedQueryVars;

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

#[derive(Deserialize)]
struct SubgraphPaginatedQueryResponse {
    meta: Meta,
    results: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct SubgraphPaginatedQueryResponseOpaqueEntry {
    id: String,
}

async fn send_query<T>(
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

    let res = builder
        .send_graphql(query)
        .await
        .map_err(|err| err.to_string())?;

    Ok(res)
}

async fn send_subgraph_query<T>(
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

async fn send_subgraph_init_query(
    client: &reqwest::Client,
    subgraph_url: Url,
    ticket: Option<&str>,
) -> Result<SubgraphInitQueryResponse, String> {
    send_query(client, subgraph_url, ticket, SUBGRAPH_INIT_QUERY_DOCUMENT)
        .await
        .map_err(|err| format!("Error sending init subgraph query: {}", err))?
        .map_err(|err| err.to_string())
}

async fn send_subgraph_paginated_query(
    client: &reqwest::Client,
    subgraph_url: Url,
    ticket: Option<&str>,
    query: SubgraphPaginatedQuery,
) -> Result<ResponseResult<SubgraphPaginatedQueryResponse>, String> {
    send_query(client, subgraph_url, ticket, query)
        .await
        .map_err(|err| format!("Error sending subgraph graphql query: {}", err))
}

/// A client for interacting with a subgraph.
pub struct Client {
    http_client: reqwest::Client,
    subgraph_endpoint: Url,
    ticket: Option<String>,

    /// The latest block number that the subgraph has progressed to.
    /// This is set to 0 initially and updated after each paginated query.
    latest_block: BlockNumber,
}

impl Client {
    pub fn new(
        http_client: reqwest::Client,
        subgraph_endpoint: Url,
        ticket: Option<String>,
    ) -> Self {
        Self {
            http_client,
            subgraph_endpoint,
            ticket,
            latest_block: 0,
        }
    }

    pub async fn query<T: for<'de> Deserialize<'de>>(
        &self,
        query: impl IntoRequestParameters + Send,
    ) -> Result<T, String> {
        let response = send_subgraph_query::<T>(
            &self.http_client,
            self.subgraph_endpoint.clone(),
            self.ticket.as_deref(),
            query,
        )
        .await?;

        Ok(response)
    }

    pub async fn paginated_query<T: for<'de> Deserialize<'de>>(
        &mut self,
        query: impl IntoDocument + Clone,
    ) -> Result<Vec<T>, String> {
        let batch_size: u32 = 200;

        // graph-node is rejecting values of `number_gte:0` on subgraphs with a larger `startBlock`
        // TODO: delete when resolved
        if self.latest_block == 0 {
            let init = send_subgraph_init_query(
                &self.http_client,
                self.subgraph_endpoint.clone(),
                self.ticket.as_deref(),
            )
            .await?;

            self.latest_block = init.meta.block.number;
        }

        // The last id of the previous batch.
        let mut last_id = "".to_string();
        // The block at which the query should be executed.
        let mut query_block: Option<BlockPointer> = None;

        // Vector to store the results of the paginated query.
        let mut results = Vec::new();

        loop {
            let block = query_block
                .as_ref()
                .map(|block| BlockHeight {
                    hash: Some(block.hash),
                    ..Default::default()
                })
                .unwrap_or({
                    BlockHeight {
                        number_gte: Some(self.latest_block),
                        ..Default::default()
                    }
                });

            let response = send_subgraph_paginated_query(
                &self.http_client,
                self.subgraph_endpoint.clone(),
                self.ticket.as_deref(),
                SubgraphPaginatedQuery::new(
                    query.clone(),
                    SubgraphPaginatedQueryVars {
                        block,
                        first: batch_size,
                        last: last_id,
                    },
                ),
            )
            .await?;

            let data = match response {
                Ok(data) if !data.results.is_empty() => data,
                Ok(_) => break,
                Err(err) => match err {
                    ResponseError::Empty => break,
                    ResponseError::Failure { errors } => {
                        let errors = errors
                            .into_iter()
                            .map(|err| err.message)
                            .collect::<Vec<String>>();

                        if errors
                            .iter()
                            .any(|err| err.contains("no block with that hash found"))
                        {
                            tracing::info!("Reorg detected. Restarting query to try a new block.");

                            last_id = "".to_string();
                            query_block = None;
                            continue;
                        }

                        return Err(errors.join(", "));
                    }
                },
            };

            last_id = serde_json::from_str::<SubgraphPaginatedQueryResponseOpaqueEntry>(
                data.results.last().unwrap().get(),
            )
            .map_err(|_| "failed to extract id for last entry".to_string())?
            .id;
            query_block = Some(data.meta.block);

            for entry in data.results {
                results
                    .push(serde_json::from_str::<T>(entry.get()).map_err(|err| err.to_string())?);
            }
        }

        if let Some(block) = query_block {
            self.latest_block = block.number;
        }

        Ok(results)
    }
}
