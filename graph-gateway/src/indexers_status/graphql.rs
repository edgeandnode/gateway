use serde::de::DeserializeOwned;
use serde_json::json;
use toolshed::graphql::http::Response;
use toolshed::url::Url;

/// Trait for types that can be converted into a GraphQL query.
pub trait IntoGraphqlQuery {
    fn to_query(&self) -> String;
}

/// Send a GraphQL query to a given URL.
///
/// This function is a wrapper around `reqwest::Client` that:
///  - Sets the `Content-Type` header to `application/json`.
///  - Sets the request body to the given query.
///  - Deserializes the response body into the given type.
// TODO: Improve error handling. Define custom error enum.
pub async fn send_graphql_query<T>(
    client: &reqwest::Client,
    url: Url,
    query: impl IntoGraphqlQuery,
) -> anyhow::Result<T>
where
    T: DeserializeOwned,
{
    let query = query.to_query();
    let body = &json!({ "query": query });

    let response = client.post(url.0).json(body).send().await?;

    let status = response.status();
    let body = response
        .json::<Response<T>>()
        .await
        .map_err(|err| anyhow::anyhow!("Response body deserialization failed: {}", err))?;

    // The GraphQL server returns a 400  if the query is invalid together with a JSON object
    // containing the error message.
    body.unpack()
        .map_err(|err| anyhow::anyhow!("GraphQL query failed with status {}: {}", status, err))
}
