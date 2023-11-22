use alloy_primitives::aliases::BlockNumber;
use graphql_http::graphql::IntoDocument;
use graphql_http::http::request::IntoRequestParameters;
use graphql_http::http_client::ResponseError;
use serde::de::Deserialize;
use toolshed::thegraph::block_pointer::BlockPointer;
use toolshed::url::Url;

use super::queries::{
    meta::send_subgraph_meta_query,
    page::{send_subgraph_page_query, BlockHeight, SubgraphPageQueryResponseOpaqueEntry},
    send_subgraph_query,
};

const DEFAULT_SUBGRAPH_PAGE_QUERY_SIZE: usize = 200;

async fn send_paginated_query<T: for<'de> Deserialize<'de>>(
    client: &reqwest::Client,
    subgraph_url: Url,
    query: impl IntoDocument + Clone,
    ticket: Option<&str>,
    batch_size: usize,
    latest_block: BlockNumber,
) -> Result<(Vec<T>, BlockNumber), String> {
    // The latest block number that the subgraph has progressed to.
    let mut latest_block = latest_block;
    // The last id of the previous batch.
    let mut last_id: Option<String> = None;
    // The block at which the query should be executed.
    let mut query_block: Option<BlockPointer> = None;

    // Vector to store the results of the paginated query.
    let mut results = Vec::new();

    loop {
        let block_height = match query_block.as_ref() {
            Some(block) => BlockHeight::new_with_block_hash(block.hash),
            None => BlockHeight::new_with_block_number_gte(latest_block),
        };

        let response = send_subgraph_page_query(
            client,
            subgraph_url.clone(),
            ticket,
            query.clone(),
            block_height,
            batch_size,
            last_id,
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

                        last_id = None;
                        query_block = None;
                        continue;
                    }

                    return Err(errors.join(", "));
                }
            },
        };

        last_id = Some(
            serde_json::from_str::<SubgraphPageQueryResponseOpaqueEntry>(
                data.results.last().unwrap().get(),
            )
            .map_err(|_| "failed to extract id for last entry".to_string())?
            .id,
        );
        query_block = Some(data.meta.block);

        for entry in data.results {
            results.push(serde_json::from_str::<T>(entry.get()).map_err(|err| err.to_string())?);
        }
    }

    if let Some(block) = query_block {
        latest_block = block.number;
    }

    Ok((results, latest_block))
}

/// A client for interacting with a subgraph.
pub struct Client {
    http_client: reqwest::Client,
    subgraph_url: Url,

    /// The request authentication bearer token.
    ///
    /// This is token is inserted in the `Authentication` header.
    auth_token: Option<String>,

    /// The latest block number that the subgraph has progressed to.
    /// This is set to 0 initially and updated after each paginated query.
    latest_block: BlockNumber,

    /// The number of entities to fetch per paginated query.
    page_size: usize,
}

impl Client {
    /// Create a new client with default settings.
    ///
    /// The default settings are:
    /// - No authentication token
    /// - Page size of 200 entities per query
    /// - Latest block number of 0
    pub fn new(http_client: reqwest::Client, subgraph_url: Url) -> Self {
        Self {
            http_client,
            subgraph_url,
            auth_token: None,
            latest_block: 0,
            page_size: DEFAULT_SUBGRAPH_PAGE_QUERY_SIZE,
        }
    }

    /// Create a new client builder.
    ///
    /// The builder allows for configuring the client before building it.
    ///
    /// Example:
    /// ```text
    /// let client = SubgraphClient::builder(http_client, subgraph_url)
    ///     .with_auth_token(Some(ticket))
    ///     .with_page_size(100)
    ///     .with_subgraph_latest_block(18627000)
    ///     .build();
    /// ```
    pub fn builder(http_client: reqwest::Client, subgraph_url: Url) -> ClientBuilder {
        ClientBuilder::new(http_client, subgraph_url)
    }

    pub async fn query<T: for<'de> Deserialize<'de>>(
        &self,
        query: impl IntoRequestParameters + Send,
    ) -> Result<T, String> {
        send_subgraph_query::<T>(
            &self.http_client,
            self.subgraph_url.clone(),
            self.auth_token.as_deref(),
            query,
        )
        .await
    }

    pub async fn paginated_query<T: for<'de> Deserialize<'de>>(
        &mut self,
        query: impl IntoDocument + Clone,
    ) -> Result<Vec<T>, String> {
        // Graph-node is rejecting values of `number_gte:0` on subgraphs with a larger `startBlock`.
        // This forces us to request the latest block number from the subgraph before sending the
        // paginated query.
        // TODO: delete when resolved
        if self.latest_block == 0 {
            let init = send_subgraph_meta_query(
                &self.http_client,
                self.subgraph_url.clone(),
                self.auth_token.as_deref(),
            )
            .await?;

            self.latest_block = init.meta.block.number;
        }

        // Send the paginated query.
        let (results, latest_block) = send_paginated_query(
            &self.http_client,
            self.subgraph_url.clone(),
            query,
            self.auth_token.as_deref(),
            self.page_size,
            self.latest_block,
        )
        .await?;

        self.latest_block = latest_block;

        Ok(results)
    }
}

/// A builder for constructing a subgraph client.
pub struct ClientBuilder {
    http_client: reqwest::Client,
    subgraph_url: Url,
    auth_token: Option<String>,
    latest_block: BlockNumber,
    page_size: usize,
}

impl ClientBuilder {
    fn new(http_client: reqwest::Client, subgraph_url: Url) -> Self {
        Self {
            http_client,
            subgraph_url,
            auth_token: None,
            latest_block: 0,
            page_size: DEFAULT_SUBGRAPH_PAGE_QUERY_SIZE,
        }
    }

    /// Set request authentication token.
    ///
    /// By default all requests are issued non-authenticated.
    pub fn with_auth_token(mut self, token: Option<String>) -> Self {
        self.auth_token = token;
        self
    }

    /// Set the number of entities to fetch per page query.
    ///
    /// The default value is 200 entities per query.
    pub fn with_page_size(mut self, size: usize) -> Self {
        debug_assert_ne!(size, 0, "page size must be greater than 0");
        self.page_size = size;
        self
    }

    /// Set the latest block number that the subgraph has progressed to.
    ///
    /// The default value is 0.
    pub fn with_subgraph_latest_block(mut self, latest_block: BlockNumber) -> Self {
        self.latest_block = latest_block;
        self
    }

    pub fn build(self) -> Client {
        Client {
            http_client: self.http_client,
            subgraph_url: self.subgraph_url,
            auth_token: self.auth_token,
            latest_block: self.latest_block,
            page_size: self.page_size,
        }
    }
}
