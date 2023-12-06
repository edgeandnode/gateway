use std::{iter, time::Duration};

use axum::Json;
use eventuals::{Eventual, EventualExt as _, Ptr};
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use thegraph::client as subgraph_client;
use tokio::sync::Mutex;

pub mod auth;
pub mod block_constraints;
pub mod budgets;
pub mod chains;
pub mod client_query;
pub mod config;
pub mod geoip;
pub mod indexer_client;
pub mod indexers_status;
pub mod indexing;
pub mod indexings_blocklist;
pub mod ipfs;
pub mod metrics;
pub mod network_subgraph;
pub mod poi;
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

pub fn spawn_poller<T>(
    client: subgraph_client::Client,
    query: String,
    label: &'static str,
    interval: Duration,
) -> Eventual<Ptr<Vec<T>>>
where
    T: for<'de> Deserialize<'de> + Send + 'static,
    Ptr<Vec<T>>: Send,
{
    let (writer, reader) = Eventual::new();
    let state: &'static Mutex<_> = Box::leak(Box::new(Mutex::new((writer, client))));
    eventuals::timer(interval)
        .pipe_async(move |_| {
            let query = query.clone();
            async move {
                let mut guard = state.lock().await;
                match guard.1.paginated_query::<T>(query).await {
                    Ok(response) => guard.0.write(Ptr::new(response)),
                    Err(subgraph_poll_err) => {
                        tracing::error!(label, %subgraph_poll_err);
                    }
                };
            }
        })
        .forever();
    reader
}
