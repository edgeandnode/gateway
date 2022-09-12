use crate::URL;
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value as JSON;

pub async fn query<T>(client: &Client, url: URL, body: &JSON) -> Result<Response<T>, String>
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
pub struct Response<T> {
    pub data: Option<T>,
    pub errors: Option<Vec<Error>>,
}

#[derive(Deserialize)]
pub struct Error {
    pub message: String,
}

impl<T> Response<T> {
    pub fn unpack(self) -> Result<T, String> {
        self.data.ok_or_else(|| {
            self.errors
                .unwrap_or_default()
                .into_iter()
                .map(|err| err.message)
                .collect::<Vec<String>>()
                .join(", ")
        })
    }
}
