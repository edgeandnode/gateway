use crate::{
    indexer_selection::{Indexers, UnresolvedBlock},
    prelude::*,
    ws_client,
};
use reqwest;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{json, Value as JSON};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    self,
    sync::{mpsc, oneshot},
    time::{interval, Duration, Interval},
};
use tracing::{self, Instrument};

pub enum Request {
    Block(UnresolvedBlock, oneshot::Sender<BlockPointer>),
}

enum Source {
    WS(ws_client::Interface),
    REST(Interval),
}

struct Client {
    network: String,
    url: String,
    indexers: Arc<Indexers>,
    rest_client: reqwest::Client,
    source: Source,
    requests: mpsc::Receiver<Request>,
    request_id: usize,
}

pub fn create(network: String, url: String, indexers: Arc<Indexers>) -> mpsc::Sender<Request> {
    let _trace = tracing::info_span!("Alchemy client", %network).entered();
    let buffer = 32;
    let (request_send, request_recv) = mpsc::channel::<Request>(buffer);
    let mut client = Client {
        network,
        source: Source::WS(ws_client::create(buffer, format!("wss://{}", url), 3)),
        url,
        indexers,
        rest_client: reqwest::Client::new(),
        requests: request_recv,
        request_id: 0,
    };
    tokio::spawn(
        async move {
            while let Ok(()) = client.run().await {}
            tracing::error!("exit");
        }
        .in_current_span(),
    );
    request_send
}

impl Client {
    async fn run(&mut self) -> Result<(), ()> {
        match &mut self.source {
            Source::WS(ws) => {
                tokio::select! {
                    msg = ws.recv.recv() => match msg {
                        Some(msg) => self.handle_ws_msg(msg).await,
                        None => self.fallback_to_rest(),
                    },
                    req = self.requests.recv() => self.handle_request(req).await,
                }
            }
            Source::REST(timer) => {
                tokio::select! {
                    _ = timer.tick() => {
                        if let Some(head) = self.fetch_block(None).await {
                            self.handle_head(head).await;
                        }
                    },
                    req = self.requests.recv() => self.handle_request(req).await,
                }
            }
        }
        Ok(())
    }

    async fn handle_ws_msg(&mut self, msg: ws_client::Msg) {
        match msg {
            ws_client::Msg::Connected => {
                let sub =
                    serde_json::to_string(&self.post_body("eth_subscribe", &["newHeads".into()]))
                        .expect("Alchemy subscription should serialize to JSON");
                if let Source::WS(ws) = &mut self.source {
                    if let Err(_) = ws.send.send(ws_client::Request::Send(sub)).await {
                        self.fallback_to_rest();
                    };
                } else {
                    self.fallback_to_rest();
                }
            }
            ws_client::Msg::Recv(msg) => {
                match serde_json::from_str::<APIResponse<APIBlockHead>>(&msg) {
                    Ok(APIResponse {
                        params: Some(APIResult { result: head }),
                        ..
                    }) => self.handle_head(head).await,
                    Ok(unexpected_response) => tracing::warn!(?unexpected_response),
                    Err(err) => tracing::error!(%err),
                }
            }
        };
    }

    async fn handle_request(&mut self, request: Option<Request>) {
        let (unresolved, resolved) = match request {
            Some(Request::Block(unresolved, resolved)) => (unresolved, resolved),
            None => return,
        };
        if let Some(APIBlockHead { hash, number, .. }) = self.fetch_block(Some(unresolved)).await {
            let _ = resolved.send(BlockPointer { number, hash });
        }
    }

    fn fallback_to_rest(&mut self) {
        tracing::warn!("fallback to REST");
        // TODO: Spawn a task to reconnect WS client after some time.
        self.source = Source::REST(interval(Duration::from_secs(8)));
    }

    async fn fetch_block(&mut self, block: Option<UnresolvedBlock>) -> Option<APIBlockHead> {
        let (method, param) = match block {
            Some(UnresolvedBlock::WithHash(hash)) => {
                ("eth_getBlockByNumber", format!("{:?}", hash).into())
            }
            Some(UnresolvedBlock::WithNumber(number)) => ("eth_getBlockByNumber", number.into()),
            None => ("eth_getBlockByNumber", "latest".into()),
        };
        let response = match self
            .rest_client
            .post(format!("https://{}", self.url))
            .json(&self.post_body(method, &[param, false.into()]))
            .send()
            .await
        {
            Ok(response) => response,
            Err(post_err) => {
                tracing::error!(%post_err);
                return None;
            }
        };
        match response.json::<APIResult<APIBlockHead>>().await {
            Ok(msg) => Some(msg.result),
            Err(post_body_err) => {
                tracing::error!(%post_body_err);
                None
            }
        }
    }

    async fn handle_head(&mut self, head: APIBlockHead) {
        tracing::info!(?head);
        let APIBlockHead {
            hash,
            number,
            uncles,
        } = head;
        self.indexers
            .set_block(&self.network, BlockPointer { number, hash })
            .await;
        for uncle in uncles {
            self.indexers.remove_block(&self.network, &uncle).await;
        }
    }

    fn post_body(&mut self, method: &str, params: &[JSON]) -> JSON {
        self.request_id += 1;
        json!({
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params,
        })
    }
}

#[derive(Debug, Deserialize)]
struct APIResponse<T> {
    jsonrpc: String,
    method: Option<String>,
    result: Option<String>,
    params: Option<APIResult<T>>,
    error: Option<HashMap<String, JSON>>,
}

#[derive(Debug, Deserialize)]
struct APIResult<T> {
    result: T,
}

#[derive(Debug, Deserialize)]
struct APIBlockHead {
    #[serde(deserialize_with = "deserialize_hash")]
    hash: Bytes32,
    #[serde(deserialize_with = "deserialize_u64")]
    number: u64,
    #[serde(
        deserialize_with = "deserialize_hashes",
        skip_serializing_if = "Vec::is_empty",
        default
    )]
    uncles: Vec<Bytes32>,
}

fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let input = String::deserialize(deserializer)?;
    u64::from_str_radix(input.split_at(2).1, 16).map_err(D::Error::custom)
}

fn deserialize_hash<'de, D>(deserializer: D) -> Result<Bytes32, D::Error>
where
    D: Deserializer<'de>,
{
    let input = String::deserialize(deserializer)?;
    input.parse::<Bytes32>().map_err(Error::custom)
}

fn deserialize_hashes<'de, D>(deserializer: D) -> Result<Vec<Bytes32>, D::Error>
where
    D: Deserializer<'de>,
{
    let inputs = Vec::deserialize(deserializer)?;
    inputs
        .into_iter()
        .map(|input: &str| input.parse::<Bytes32>().map_err(Error::custom))
        .collect()
}
