use prelude::{graphql::http::Response, *};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, value::RawValue, Value};

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
        query: &str,
    ) -> Result<Vec<T>, String> {
        let batch_size: u32 = 1000;
        let mut last_id = "".to_string();
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
            let response = graphql_query::<PaginatedQueryResponse>(
                &self.http_client,
                self.subgraph_endpoint.clone(),
                &json!({
                    "query": format!(r#"
                        query q($block: Block_height!, $first: Int!, $last: String!) {{
                            meta: _meta(block: $block) {{ block {{ number hash }} }}
                            results: {query}
                        }}"#,
                    ),
                    "variables": {
                        "block": block,
                        "first": batch_size,
                        "last": last_id,
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
                last_id = "".to_string();
                query_block = None;
                continue;
            }
            if !errors.is_empty() {
                return Err(errors.join(", "));
            }
            let data = match response.data {
                Some(data) if !data.results.is_empty() => data,
                _ => break,
            };
            last_id = serde_json::from_str::<OpaqueEntry>(data.results.last().unwrap().get())
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
struct PaginatedQueryResponse {
    meta: Meta,
    results: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct OpaqueEntry {
    id: String,
}