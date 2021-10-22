use crate::{
    indexer_selection::{Indexers, UnresolvedBlock},
    prelude::*,
    query_engine::BlockHead,
    ws_client,
};
use prometheus;
use reqwest;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{json, Value as JSON};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    self,
    time::{interval, Interval},
};
use tracing::{self, Instrument};

#[derive(Debug)]
pub enum Msg {
    Request(UnresolvedBlock, oneshot::Sender<BlockHead>),
    Resolved(Option<UnresolvedBlock>, BlockHead),
}

enum Source {
    WS(ws_client::Interface),
    REST(Interval),
}

struct Client {
    network: String,
    url: String,
    indexers: Arc<Indexers>,
    metrics: Metrics,
    rest_client: reqwest::Client,
    source: Source,
    msgs: (mpsc::Sender<Msg>, mpsc::Receiver<Msg>),
    resolving: HashMap<UnresolvedBlock, Vec<oneshot::Sender<BlockHead>>>,
}

#[derive(Clone)]
pub struct Metrics {
    pub head_block: prometheus::IntGauge,
}

pub fn create(
    network: String,
    url: String,
    indexers: Arc<Indexers>,
) -> (mpsc::Sender<Msg>, Metrics) {
    let _trace = tracing::info_span!("Alchemy client", %network).entered();
    let metrics = Metrics {
        head_block: prometheus::register_int_gauge!(
            format!("head_block_{}", network),
            format!("{} head block number", network)
        )
        .unwrap(),
    };
    let buffer = 32;
    let (msg_send, msg_recv) = mpsc::channel::<Msg>(buffer);
    let mut client = Client {
        network,
        source: Source::WS(ws_client::create(buffer, format!("wss://{}", url), 3)),
        url,
        indexers,
        metrics: metrics.clone(),
        rest_client: reqwest::Client::new(),
        msgs: (msg_send.clone(), msg_recv),
        resolving: HashMap::new(),
    };
    tokio::spawn(
        async move {
            while let Ok(()) = client.run().await {}
            tracing::error!("exit");
        }
        .in_current_span(),
    );
    (msg_send, metrics)
}

impl Client {
    #[tracing::instrument(skip(self))]
    async fn run(&mut self) -> Result<(), ()> {
        match &mut self.source {
            Source::WS(ws) => {
                tokio::select! {
                    msg = ws.recv.recv() => match msg {
                        Some(msg) => self.handle_ws_msg(msg).await,
                        None => self.fallback_to_rest(),
                    },
                    msg = self.msgs.1.recv() => self.handle_msg(msg).await,
                }
            }
            Source::REST(timer) => {
                tokio::select! {
                    _ = timer.tick() => self.fetch_block(None),
                    msg = self.msgs.1.recv() => self.handle_msg(msg).await,
                }
            }
        }
        Ok(())
    }

    async fn handle_ws_msg(&mut self, msg: ws_client::Msg) {
        match msg {
            ws_client::Msg::Connected => {
                let sub =
                    serde_json::to_string(&Self::post_body("eth_subscribe", &["newHeads".into()]))
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
                    }) => self.handle_head(head.into(), true).await,
                    // Ignore subscription confirmation (hex code response)
                    // See https://docs.alchemy.com/alchemy/guides/using-websockets#4.-newheads
                    Ok(APIResponse {
                        result: Some(text),
                        error: None,
                        ..
                    }) if text.starts_with("0x") && hex::decode(&text[2..]).is_ok() => (),
                    Ok(unexpected_response) => tracing::warn!(?unexpected_response),
                    Err(err) => tracing::error!(%err),
                }
            }
        };
    }

    #[tracing::instrument(skip(self, msg))]
    async fn handle_msg(&mut self, msg: Option<Msg>) {
        match msg {
            Some(Msg::Request(unresolved, notify)) => {
                let notifies = self
                    .resolving
                    .entry(unresolved.clone())
                    .or_insert_with(move || Vec::with_capacity(1));
                let first = notifies.is_empty();
                notifies.push(notify);
                if first {
                    self.fetch_block(Some(unresolved));
                }
            }
            Some(Msg::Resolved(key, head)) => {
                self.handle_head(head.clone(), key.is_none()).await;
                if let Some(key) = key {
                    if let Some(notifies) = self.resolving.remove(&key) {
                        for notify in notifies {
                            let _ = notify.send(head.clone());
                        }
                    }
                }
            }
            None => return,
        }
    }

    fn fallback_to_rest(&mut self) {
        tracing::warn!("fallback to REST");
        // TODO: Spawn a task to reconnect WS client after some time.
        self.source = Source::REST(interval(Duration::from_secs(8)));
    }

    #[tracing::instrument(skip(self))]
    fn fetch_block(&self, block: Option<UnresolvedBlock>) {
        let rest_client = self.rest_client.clone();
        let url = self.url.clone();
        let msg_send = self.msgs.0.clone();
        tokio::spawn(
            async move {
                let (method, param) = match &block {
                    Some(UnresolvedBlock::WithHash(hash)) => {
                        ("eth_getBlockByHash", hash.to_string().into())
                    }
                    Some(UnresolvedBlock::WithNumber(number)) => {
                        ("eth_getBlockByNumber", format!("0x{:x}", number).into())
                    }
                    None => ("eth_getBlockByNumber", "latest".into()),
                };
                tracing::debug!(%method, %param);
                let response = match rest_client
                    .post(format!("https://{}", url))
                    .json(&Self::post_body(method, &[param, false.into()]))
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(post_err) => {
                        tracing::error!(%post_err);
                        return;
                    }
                };
                let head = match response.json::<APIResult<APIBlockHead>>().await {
                    Ok(msg) => msg.result,
                    Err(post_body_err) => {
                        tracing::error!(%post_body_err);
                        return;
                    }
                };
                let _ = msg_send.send(Msg::Resolved(block, head.into())).await;
            }
            .in_current_span(),
        );
    }

    async fn handle_head(&mut self, head: BlockHead, latest: bool) {
        tracing::info!(?head);
        if latest {
            self.metrics.head_block.set(head.block.number as i64);
        }
        self.indexers.set_block(&self.network, head.block).await;
        for uncle in head.uncles {
            self.indexers.remove_block(&self.network, &uncle).await;
        }
    }

    fn post_body(method: &str, params: &[JSON]) -> JSON {
        json!({
            "jsonrpc": "2.0",
            "id": 0,
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
    hash: Bytes32,
    #[serde(deserialize_with = "deserialize_u64")]
    number: u64,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    uncles: Vec<Bytes32>,
}

fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let input = String::deserialize(deserializer)?;
    u64::from_str_radix(input.split_at(2).1, 16).map_err(D::Error::custom)
}

impl From<APIBlockHead> for BlockHead {
    fn from(from: APIBlockHead) -> Self {
        Self {
            block: BlockPointer {
                hash: from.hash,
                number: from.number,
            },
            uncles: from.uncles,
        }
    }
}
