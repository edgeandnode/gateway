use std::time::Duration;

use thegraph_core::types::BlockPointer;
use tokio::sync::mpsc;

use crate::chains::BlockHead;

use super::{ClientMsg, UnresolvedBlock};

pub struct Config {
    pub chain: String,
    pub blocks: Vec<BlockPointer>,
}

pub struct Client;

impl super::Client for Client {
    type Config = Config;

    fn chain_name(config: &Self::Config) -> &str {
        &config.chain
    }

    fn poll_interval() -> std::time::Duration {
        Duration::from_secs(1)
    }

    fn create(
        config: Config,
        notify: mpsc::UnboundedSender<ClientMsg>,
    ) -> mpsc::UnboundedSender<UnresolvedBlock> {
        let (tx, mut rx) = mpsc::unbounded_channel::<UnresolvedBlock>();
        tokio::spawn(async move {
            if let Some(head) = config.blocks.last() {
                notify
                    .send(ClientMsg::Head(BlockHead {
                        block: head.clone(),
                        uncles: vec![],
                    }))
                    .unwrap();
            }
            loop {
                tokio::select! {
                    Some(unresolved) = rx.recv() => {
                        match config.blocks.iter().find(|b| unresolved.matches(b)) {
                            Some(block) => notify.send(ClientMsg::Block(block.clone())).unwrap(),
                            None => notify.send(ClientMsg::Err(unresolved)).unwrap(),
                        };
                    }
                    else => break,
                }
            }
            unreachable!("test chain client exit");
        });
        tx
    }
}
