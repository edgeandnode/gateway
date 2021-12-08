use crate::{
    block_resolver::BlockCacheWriter, indexer_selection::UnresolvedBlock, prelude::*, ws_client,
};
use lazy_static::lazy_static;
use prometheus;
use reqwest;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{json, Value as JSON};
use std::collections::HashMap;
use tokio::{
    self,
    time::{interval, sleep, Interval},
};
use tracing::{self, Instrument};
use url::Url;

const MPSC_BUFFER: usize = 32;
const WS_RETRY_LIMIT: usize = 3;
const REST_POLL_INTERVAL: Duration = Duration::from_secs(8);

#[derive(Debug)]
pub struct Provider {
    pub network: String,
    pub rest_url: Url,
    pub websocket_url: Option<Url>,
}

#[derive(Debug)]
pub enum Msg {
    Request(UnresolvedBlock, oneshot::Sender<BlockPointer>),
    Resolved(Option<UnresolvedBlock>, BlockHead),
    RetryWS,
}

enum Source {
    WS(ws_client::Interface),
    REST(Interval),
}

struct Client {
    provider: Provider,
    rest_client: reqwest::Client,
    source: Source,
    msgs: (mpsc::Sender<Msg>, mpsc::Receiver<Msg>),
    resolving: HashMap<UnresolvedBlock, Vec<oneshot::Sender<BlockPointer>>>,
    block_cache: BlockCacheWriter,
}

pub fn create(provider: Provider, block_cache: BlockCacheWriter) -> mpsc::Sender<Msg> {
    let _trace = tracing::info_span!("Ethereum client", network = %provider.network).entered();
    let (msg_send, msg_recv) = mpsc::channel::<Msg>(MPSC_BUFFER);
    let mut client = Client {
        source: Client::try_ws_source(&provider),
        provider,
        rest_client: reqwest::Client::new(),
        msgs: (msg_send.clone(), msg_recv),
        resolving: HashMap::new(),
        block_cache,
    };
    tokio::spawn(
        async move {
            while let Ok(()) = client.run().await {}
            tracing::error!("exit");
        }
        .in_current_span(),
    );
    msg_send
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
                        .expect("Ethereum subscription should serialize to JSON");
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
                            let _ = notify.send(head.block.clone());
                        }
                    }
                }
            }
            Some(Msg::RetryWS) => {
                tracing::info!("retry WS source");
                self.source = Self::try_ws_source(&self.provider);
            }
            None => return,
        }
    }

    fn fallback_to_rest(&mut self) {
        tracing::warn!("fallback to REST");
        self.source = Source::REST(interval(REST_POLL_INTERVAL));
        let recv = self.msgs.0.clone();
        tokio::spawn(async move {
            // Retry WS connection after 20 minutes
            sleep(Duration::from_secs(60 * 20)).await;
            let _ = recv.send(Msg::RetryWS).await;
        });
    }

    fn try_ws_source(provider: &Provider) -> Source {
        provider
            .websocket_url
            .as_ref()
            .map(|ws_url| {
                Source::WS(ws_client::create(
                    MPSC_BUFFER,
                    ws_url.clone(),
                    WS_RETRY_LIMIT,
                ))
            })
            .unwrap_or(Source::REST(interval(REST_POLL_INTERVAL)))
    }

    #[tracing::instrument(skip(self))]
    fn fetch_block(&self, block: Option<UnresolvedBlock>) {
        let rest_client = self.rest_client.clone();
        let url = self.provider.rest_url.clone();
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
                    .post(url)
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
            with_metric(&METRICS.head_block, &[&self.provider.network], |g| {
                g.set(head.block.number as i64)
            });
        }
        self.block_cache.insert(head.block, &head.uncles);
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

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
}

struct Metrics {
    head_block: prometheus::IntGaugeVec,
}

impl Metrics {
    fn new() -> Self {
        Self {
            head_block: prometheus::register_int_gauge_vec!(
                "head_block",
                "Chain head block number",
                &["network"]
            )
            .unwrap(),
        }
    }
}
