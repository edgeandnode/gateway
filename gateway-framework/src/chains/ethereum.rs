use std::time::Duration;

use alloy_primitives::BlockHash;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{json, Value as JSON};
use thegraph::types::BlockPointer;
use tokio::sync::mpsc;
use tokio::time::interval;
use toolshed::url::Url;
use tracing::Instrument;

use super::{BlockHead, ClientMsg, UnresolvedBlock};
use crate::{config, metrics::METRICS};

pub struct Client {
    chain: config::Chain,
    http_client: reqwest::Client,
    notify: mpsc::UnboundedSender<ClientMsg>,
}

impl super::Client for Client {
    type Config = config::Chain;

    fn chain_name(config: &Self::Config) -> &str {
        &config.names[0]
    }

    fn poll_interval() -> Duration {
        Duration::from_secs(2)
    }

    fn create(
        chain: config::Chain,
        notify: mpsc::UnboundedSender<ClientMsg>,
    ) -> mpsc::UnboundedSender<UnresolvedBlock> {
        let _trace = tracing::info_span!("ethereum_client", chain = %chain.names[0]).entered();
        let (unresolved_tx, mut unresolved_rx) = mpsc::unbounded_channel();
        let mut client = Self {
            chain,
            http_client: reqwest::Client::new(),
            notify,
        };
        tokio::spawn(
            async move {
                let mut poll_timer = interval(Self::poll_interval());
                loop {
                    tokio::select! {
                        _ = poll_timer.tick() => {
                            client.spawn_block_fetch(None).await;
                        },
                        Some(unresolved) = unresolved_rx.recv() => {
                            client.spawn_block_fetch(Some(unresolved)).await;
                        },
                        else => break,
                    }
                }
                tracing::error!("Ethereum client exit");
            }
            .in_current_span(),
        );
        unresolved_tx
    }
}

impl Client {
    async fn spawn_block_fetch(&mut self, unresolved: Option<UnresolvedBlock>) {
        let client = self.http_client.clone();
        let chain = self.chain.names[0].clone();
        let rpc = self.chain.rpc.clone();
        let notify = self.notify.clone();
        tokio::spawn(async move {
            let timer = METRICS.block_resolution.start_timer(&[&chain]);
            let result = Self::fetch_block(client, rpc, unresolved.clone()).await;
            drop(timer);
            METRICS.block_resolution.check(&[&chain], &result);
            let response = match result {
                Ok(head) => match &unresolved {
                    Some(_) => ClientMsg::Block(head.block),
                    None => ClientMsg::Head(head),
                },
                Err(fetch_block_err) => {
                    tracing::warn!(chain, ?unresolved, %fetch_block_err);
                    match unresolved {
                        Some(unresolved) => ClientMsg::Err(unresolved),
                        None => return,
                    }
                }
            };
            let _ = notify.send(response);
        });
    }

    async fn fetch_block(
        client: reqwest::Client,
        rpc: Url,
        unresolved: Option<UnresolvedBlock>,
    ) -> anyhow::Result<BlockHead> {
        let (method, param): (&str, JSON) = match &unresolved {
            Some(UnresolvedBlock::WithHash(hash)) => {
                ("eth_getBlockByHash", hash.to_string().into())
            }
            Some(UnresolvedBlock::WithNumber(number)) => {
                ("eth_getBlockByNumber", format!("0x{number:x}").into())
            }
            None => ("eth_getBlockByNumber", "latest".into()),
        };
        tracing::trace!(%method, %param);
        let body = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": method,
            "params": &[param, false.into()],
        });
        client
            .post(rpc.0)
            .json(&body)
            .send()
            .await
            .and_then(|response| response.error_for_status())?
            .json::<APIResult<APIBlockHead>>()
            .await
            .map(|APIResult { result }| BlockHead {
                block: BlockPointer {
                    hash: result.hash,
                    number: result.number,
                },
                uncles: result.uncles,
            })
            .map_err(Into::into)
    }
}

#[derive(Debug, Deserialize)]
struct APIResult<T> {
    result: T,
}

#[derive(Debug, Deserialize)]
struct APIBlockHead {
    hash: BlockHash,
    #[serde(deserialize_with = "deserialize_u64")]
    number: u64,
    #[serde(default)]
    uncles: Vec<BlockHash>,
}

fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let input = String::deserialize(deserializer)?;
    u64::from_str_radix(input.split_at(2).1, 16).map_err(D::Error::custom)
}
