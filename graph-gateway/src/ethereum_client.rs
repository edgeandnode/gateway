use crate::{block_resolver::BlockCacheWriter, metrics::*};
use indexer_selection::UnresolvedBlock;
use prelude::*;
use reqwest;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{json, Value as JSON};
use std::collections::HashMap;
use tracing::{self, Instrument};

const MPSC_BUFFER: usize = 32;

#[derive(Debug)]
pub struct Provider {
    pub network: String,
    pub block_time: Duration,
    pub rpc: URL,
}

#[derive(Debug)]
pub enum Msg {
    Request(UnresolvedBlock, oneshot::Sender<BlockPointer>),
    Resolved(Option<UnresolvedBlock>, BlockHead),
}

struct Client {
    provider: Provider,
    rest_client: reqwest::Client,
    msgs: (mpsc::Sender<Msg>, mpsc::Receiver<Msg>),
    resolving: HashMap<UnresolvedBlock, Vec<oneshot::Sender<BlockPointer>>>,
    block_cache: BlockCacheWriter,
}

pub fn create(provider: Provider, block_cache: BlockCacheWriter) -> mpsc::Sender<Msg> {
    let _trace = tracing::info_span!("Ethereum client", network = %provider.network).entered();
    let (msg_send, msg_recv) = mpsc::channel::<Msg>(MPSC_BUFFER);
    let mut client = Client {
        provider,
        rest_client: reqwest::Client::new(),
        msgs: (msg_send.clone(), msg_recv),
        resolving: HashMap::new(),
        block_cache,
    };
    tokio::spawn(
        async move {
            let mut timer = tokio::time::interval(client.provider.block_time);
            loop {
                tokio::select! {
                    _ = timer.tick() => client.fetch_block(None),
                    msg = client.msgs.1.recv() => client.handle_msg(msg).await,
                }
            }
        }
        .in_current_span(),
    );
    msg_send
}

impl Client {
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
            None => return,
        }
    }

    #[tracing::instrument(skip(self))]
    fn fetch_block(&self, block: Option<UnresolvedBlock>) {
        let rest_client = self.rest_client.clone();
        let url = self.provider.rpc.clone();
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
                tracing::trace!(%method, %param);
                let response = match rest_client
                    .post(url.0)
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
            with_metric(&METRICS.chain_head, &[&self.provider.network], |g| {
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
