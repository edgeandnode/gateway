use prelude::{graphql::http::Response, *};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

pub struct Client {
    subgraph_endpoint: Url,
    http_client: reqwest::Client,
    latest_block: u64,
}

impl Client {
    pub fn new(http_client: reqwest::Client, subgraph_endpoint: Url) -> Self {
        Self {
            subgraph_endpoint,
            http_client,
            latest_block: 0,
        }
    }

    pub async fn query<T: for<'de> Deserialize<'de>>(&self, query: &Value) -> Result<T, String> {
        let response = graphql_query::<T>(&self.http_client, self.subgraph_endpoint.clone(), query)
            .await?
            .data
            .ok_or("empty response")?;

        Ok(response)
    }

    pub async fn paginated_query<T: for<'de> Deserialize<'de>>(
        &mut self,
        query: &'static str,
    ) -> Result<Vec<T>, String> {
        let batch_size: u32 = 1000;
        let mut index: u32 = 0;
        let mut query_block: Option<BlockPointer> = None;
        let mut results = Vec::new();
        // graph-node is rejecting values of `number_gte:0` on subgraphs with a larger `startBlock`
        // TODO: delete when resolved
        if self.latest_block == 0 {
            #[derive(Deserialize)]
            struct InitResponse {
                meta: Meta,
            }
            let init = graphql_query::<InitResponse>(
                &self.http_client,
                self.subgraph_endpoint.clone(),
                &json!({"query": "{ meta: _meta { block { number hash } } }"}),
            )
            .await?
            .unpack()?;
            self.latest_block = init.meta.block.number;
        }
        loop {
            let block = query_block
                .as_ref()
                .map(|block| json!({ "hash": block.hash }))
                .unwrap_or(json!({ "number_gte": self.latest_block }));
            let response = graphql_query::<PaginatedQueryResponse<T>>(
                &self.http_client,
                self.subgraph_endpoint.clone(),
                &json!({
                    "query": format!(r#"
                        query q($block: Block_height!, $skip: Int!, $first: Int!) {{
                            meta: _meta(block: $block) {{ block {{ number hash }} }}
                            results: {query}
                        }}"#,
                    ),
                    "variables": {
                        "block": block,
                        "skip": index * batch_size,
                        "first": batch_size,
                    },
                }),
            )
            .await?;
            let errors = response
                .errors
                .unwrap_or_default()
                .into_iter()
                .map(|err| err.message)
                .collect::<Vec<String>>();
            if errors
                .iter()
                .any(|err| err.contains("no block with that hash found"))
            {
                tracing::info!("Reorg detected. Restarting query to try a new block.");
                index = 0;
                query_block = None;
                continue;
            }
            if !errors.is_empty() {
                return Err(errors.join(", "));
            }
            let mut data = match response.data {
                Some(data) if !data.results.is_empty() => data,
                _ => break,
            };
            index += 1;
            query_block = Some(data.meta.block);
            results.append(&mut data.results);
        }
        if let Some(block) = query_block {
            self.latest_block = block.number;
        }
        Ok(results)
    }
}

pub async fn graphql_query<T>(
    client: &reqwest::Client,
    url: Url,
    body: &Value,
) -> Result<Response<T>, String>
where
    T: DeserializeOwned,
{
    client
        .post(url.0)
        .json(body)
        .send()
        .await
        .map_err(|err| err.to_string())?
        .json::<Response<T>>()
        .await
        .map_err(|err| err.to_string())
}

#[derive(Deserialize)]
struct Meta {
    block: BlockPointer,
}

#[derive(Deserialize)]
struct PaginatedQueryResponse<T> {
    meta: Meta,
    results: Vec<T>,
}
