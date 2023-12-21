use std::iter;

use axum::Json;
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};

pub mod auth;
pub mod block_constraints;
pub mod budgets;
pub mod client_query;
pub mod config;
pub mod geoip;
pub mod indexer_client;
pub mod indexers;
pub mod indexings_blocklist;
pub mod ipfs;
pub mod receipts;
pub mod reports;
pub mod subgraph_studio;
pub mod subscriptions;
pub mod subscriptions_subgraph;
pub mod topology;
pub mod unattestable_errors;
pub mod vouchers;

pub type JsonResponse = (HeaderMap, Json<serde_json::Value>);

pub fn json_response<H>(headers: H, payload: serde_json::Value) -> JsonResponse
where
    H: IntoIterator<Item = (HeaderName, HeaderValue)>,
{
    let headers = HeaderMap::from_iter(
        iter::once((
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        ))
        .chain(headers),
    );
    (headers, Json(payload))
}
