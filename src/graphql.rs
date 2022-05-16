use reqwest::{Client, IntoUrl};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value as JSON;

pub async fn query<T, U>(client: &Client, url: U, body: &JSON) -> Result<Response<T>, String>
where
    T: DeserializeOwned,
    U: IntoUrl,
{
    client
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(|err| err.to_string())?
        .json::<Response<T>>()
        .await
        .map_err(|err| err.to_string())
}

#[derive(Deserialize)]
pub struct Response<T> {
    pub data: Option<T>,
    pub errors: Option<Vec<Error>>,
}

#[derive(Deserialize)]
pub struct Error {
    pub message: String,
}
