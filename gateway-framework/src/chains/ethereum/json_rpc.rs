//! A simple `reqwest`-based JSON-RPC client.

use anyhow::anyhow;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use self::types::RawResponse;
pub use self::types::Request;

/// Utility method that generates a UUIDv7 using the current unix timestamp.
pub fn new_request_id() -> String {
    Uuid::now_v7().to_string()
}

/// JSON-RPC request and response types.
mod types {
    use serde::{Deserialize, Serialize};
    use serde_json::value::RawValue;

    /// The JSON-RPC request format.
    #[derive(Debug, Serialize)]
    pub struct Request<'a> {
        /// The JSON-RPC protocol version.
        #[serde(rename = "jsonrpc")]
        version: &'a str,
        /// The request ID.
        pub(super) id: &'a str,
        /// The method to call.
        pub(super) method: &'a str,
        /// The parameters for the method.
        pub(super) params: serde_json::Value,
    }

    impl<'a> Request<'a> {
        /// Create a new JSON-RPC request with the given ID.
        pub fn new(id: &'a str, method: &'a str, params: serde_json::Value) -> Self {
            Self {
                version: "2.0",
                id,
                method,
                params,
            }
        }
    }

    /// The raw JSON-RPC response format.
    #[derive(Debug, Deserialize)]
    pub struct RawResponse<'a> {
        /// The request ID.
        pub(super) id: &'a str,
        /// The result of the request.
        #[serde(default, borrow, with = "::serde_with::rust::double_option")]
        result: Option<Option<&'a RawValue>>,
        /// The error of the request.
        #[serde(default, borrow, with = "::serde_with::rust::double_option")]
        error: Option<Option<&'a RawValue>>,
    }

    impl<'a> RawResponse<'a> {
        /// Convert the JSON-RPC response into a `Result`.
        ///
        /// If the response contains an error, return it. Otherwise, deserialize the result.
        pub fn try_into_response(self) -> anyhow::Result<Response<'a>> {
            // Report if the response has both error and result fields, or none.
            // This is only enabled in debug builds.
            #[cfg(debug_assertions)]
            {
                if self.error.is_some() && self.result.is_some() {
                    tracing::debug!("JSON-RPC response has both error and result fields");
                }

                if self.error.is_none() && self.result.is_none() {
                    tracing::debug!("JSON-RPC response has neither error nor result fields");
                }
            }

            // If the response contains an error, return it
            if let Some(Some(error)) = self.error {
                return Ok(Response {
                    id: self.id,
                    result: Err(error),
                });
            }

            // Otherwise, check the `result` field
            match self.result {
                // The `result` field is missing.
                None => Err(anyhow::anyhow!("missing result")),
                // The `result` field is present and contains `null`.
                Some(None) => Ok(Response {
                    id: self.id,
                    result: Ok(None),
                }),
                // The `result` field is present and contains a value.
                Some(Some(result)) => Ok(Response {
                    id: self.id,
                    result: Ok(Some(result)),
                }),
            }
        }
    }

    /// The JSON-RPC response format.
    #[derive(Debug)]
    pub struct Response<'a> {
        /// The response ID.
        pub(super) id: &'a str,
        /// The response result.
        result: Result<Option<&'a RawValue>, &'a RawValue>,
    }

    impl<'a> Response<'a> {
        /// Convert the JSON-RPC response into a `Result`.
        ///
        /// If the response contains an error, return it. Otherwise, deserialize the result.
        pub fn into_result(self) -> anyhow::Result<Option<&'a RawValue>> {
            match self.result {
                Ok(result) => Ok(result),
                Err(error) => Err(anyhow::anyhow!("{}", error)),
            }
        }
    }
}

/// Send a raw JSON-RPC request to the Ethereum node using the given method and parameters.
pub async fn send_request<'a, T: DeserializeOwned>(
    http_client: &reqwest::Client,
    url: &url::Url,
    request: &Request<'a>,
) -> anyhow::Result<Option<T>> {
    tracing::debug!(
        request_id = %request.id,
        url = %url,
        method = %request.method,
        "sending request"
    );

    let response = http_client
        .post(url.to_owned())
        .json(request)
        .send()
        .await
        .map_err(|err| anyhow!("failed to send rpc request: {}", err))?;

    // Read the response body
    let response = response
        .bytes()
        .await
        .map_err(|err| anyhow!("failed to read rpc response: {}", err))?;

    // Deserialize the JSON-RPC response
    let response = serde_json::from_slice::<RawResponse>(&response)
        .map_err(|err| anyhow::anyhow!("invalid rpc response: {}", err))?
        .try_into_response()
        .map_err(|err| anyhow::anyhow!("invalid rpc response: {}", err))?;

    tracing::debug!(
        url = %url,
        request_id = %response.id,
        "response received"
    );

    // Report if the request and response IDs match.
    // This is only enabled in debug builds.
    #[cfg(debug_assertions)]
    {
        if request.id != response.id {
            tracing::debug!(
                request_id = %request.id,
                response_id = %response.id,
                "request and response IDs do not match"
            );
        }
    }

    // Convert the JSON-RPC response into a `Result` and deserialize the result
    // into the expected type
    match response.into_result() {
        Err(err) => Err(anyhow::anyhow!("request failed: {}", err)),
        Ok(None) => Ok(None),
        Ok(Some(result)) => serde_json::from_str::<T>(result.get())
            .map(Some)
            .map_err(|err| anyhow::anyhow!("result deserialization error: {}", err)),
    }
}
