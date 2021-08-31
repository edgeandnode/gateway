use crate::{
    indexer_selection::{Indexers, UnresolvedBlock},
    prelude::*,
    ws_client,
};
use hex;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{json, Value as JSON};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::{self, Instrument};

pub enum Request {
    Block(UnresolvedBlock, oneshot::Receiver<BlockPointer>),
}

struct Client {
    network: String,
    ws_url: String,
    indexers: Arc<Indexers>,
    requests: mpsc::Receiver<Request>,
}

pub fn create(network: String, ws_url: String, indexers: Arc<Indexers>) -> mpsc::Sender<Request> {
    let _trace = tracing::info_span!("Alchemy client", %network).entered();
    let buffer = 32;
    let (mut ws_req_send, ws_req_recv) = mpsc::channel(8);
    let (ws_msg_send, mut ws_msg_recv) = mpsc::channel(buffer);
    let (request_send, request_recv) = mpsc::channel::<Request>(buffer);
    ws_client::create(ws_msg_send, ws_req_recv, ws_url.clone(), 3);
    let mut client = Client {
        network,
        ws_url,
        indexers,
        requests: request_recv,
    };
    tokio::spawn(
        async move { while let Ok(()) = client.run(&mut ws_req_send, &mut ws_msg_recv).await {} }
            .in_current_span(),
    );
    request_send
}

impl Client {
    async fn run(
        &mut self,
        ws_send: &mut mpsc::Sender<ws_client::Request>,
        ws_recv: &mut mpsc::Receiver<ws_client::Msg>,
    ) -> Result<(), ()> {
        tokio::select! {
            ws_result = ws_recv.recv() => match ws_result {
                Some(msg) => self.handle_ws_msg(ws_send, msg).await,
                None => {
                    tracing::error!("TODO: fallback to REST when WS disconnects?");
                    Err(())
                }
            },
            Some(Request::Block(unresolved, resolved)) = self.requests.recv() => {
                tracing::error!("TODO: handle unresolved: {:?}", unresolved);
                Err(())
            },
            else => Err(()),
        }
    }

    async fn handle_ws_msg(
        &mut self,
        ws_send: &mut mpsc::Sender<ws_client::Request>,
        msg: ws_client::Msg,
    ) -> Result<(), ()> {
        match msg {
            ws_client::Msg::Connected => {
                let sub = serde_json::to_string(&json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["newHeads"],
                }))
                .expect("Alchemy subscription should serialize to JSON");
                if let Err(_) = ws_send.send(ws_client::Request::Send(sub)).await {
                    tracing::error!("TODO: handle WS client exit");
                    return Err(());
                };
            }
            ws_client::Msg::Recv(msg) => {
                match serde_json::from_str::<APIResponse<APIBlockHead>>(&msg) {
                    Ok(APIResponse {
                        params: Some(APIResult { result: head }),
                        ..
                    }) => {
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
                    Ok(unexpected_response) => tracing::warn!(?unexpected_response),
                    Err(err) => tracing::error!(%err),
                }
            }
        };
        Ok(())
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
    let mut buf = [0u8; 32];
    hex::decode_to_slice(input.split_at(2).1, &mut buf)
        .map_err(D::Error::custom)
        .map(|()| buf.into())
}

fn deserialize_hashes<'de, D>(deserializer: D) -> Result<Vec<Bytes32>, D::Error>
where
    D: Deserializer<'de>,
{
    let inputs = Vec::deserialize(deserializer)?;
    inputs
        .into_iter()
        .map(|input: &str| {
            let mut buf = [0u8; 32];
            hex::decode_to_slice(input.split_at(2).1, &mut buf).map_err(D::Error::custom)?;
            Ok(buf.into())
        })
        .collect()
}
