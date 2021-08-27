use crate::{
    indexer_selection::{Indexers, UnresolvedBlock},
    prelude::*,
    ws_client,
};
use hex;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::{json, Value as JSON};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
use tracing::{self, Instrument};

#[derive(Debug)]
pub enum Request {
    Block(UnresolvedBlock),
}

pub fn create(network: String, ws_url: String, indexers: Arc<Indexers>) -> mpsc::Sender<Request> {
    let (sender, reqs) = mpsc::channel(32);
    tokio::spawn(
        async {
            handle_ws_client(network, ws_url, indexers, reqs).await;
            todo!("Alchemy WS client exited. Fallback to REST API");
        }
        .instrument(tracing::info_span!("Alchemy handler")),
    );
    sender
}

async fn handle_ws_client(
    network: String,
    ws_url: String,
    indexers: Arc<Indexers>,
    mut reqs: mpsc::Receiver<Request>,
) {
    let (requests, mut msgs) = ws_client::create(ws_url, 3);
    loop {
        tokio::select! {
            req = reqs.recv() => match req {
                None => continue,
                Some(Request::Block(UnresolvedBlock::WithNumber(n))) => {
                    todo!("request block number");
                }
                Some(Request::Block(UnresolvedBlock::WithHash(n))) => {
                    todo!("request block hash");
                }
            },
            msg = msgs.recv() => match msg {
                Some(ws_client::Msg::Connected) => {
                   let sub = serde_json::to_string(&json!({
                       "jsonrpc": "2.0",
                       "id": 1,
                       "method": "eth_subscribe",
                       "params": ["newHeads"],
                   }))
                   .expect("Alchemy subscription should serialize to JSON");
                   match requests.send(ws_client::Request::Send(sub)) {
                       Ok(()) => continue,
                       Err(_) => return,
                   };
                }
                Some(ws_client::Msg::Recv(msg)) => match
                    serde_json::from_str::<APIResponse<APIBlockHead>>(&msg)
                {
                    Ok(APIResponse { params: Some(APIResult { result: head }), .. }) => {
                        tracing::info!("{} head: {:?}", network, head);
                        let APIBlockHead { hash, number, uncles, } = head;
                        indexers
                            .set_block(&network, BlockPointer { number, hash })
                            .await;
                        for uncle in uncles {
                            indexers.remove_block(&network, &uncle).await;
                        }
                    }
                    Ok(response) => tracing::warn!(unexpected_response = ?response),
                    Err(err) => tracing::error!(%err),
               }
               None => return,
            },
        }
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
