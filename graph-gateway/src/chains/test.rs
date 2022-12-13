use super::ClientMsg;
use indexer_selection::UnresolvedBlock;
use prelude::*;

pub struct Provider {
    pub network: String,
    pub blocks: Vec<BlockPointer>,
}

impl super::Provider for Provider {
    fn network(&self) -> &str {
        &self.network
    }
}

pub struct Client;

impl super::Client for Client {
    type Provider = Provider;

    fn create(
        provider: Provider,
        notify: mpsc::UnboundedSender<ClientMsg>,
    ) -> mpsc::UnboundedSender<UnresolvedBlock> {
        let (tx, mut rx) = mpsc::unbounded_channel::<UnresolvedBlock>();
        tokio::spawn(async move {
            if let Some(head) = provider.blocks.last() {
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
                        match provider.blocks.iter().find(|b| unresolved.matches(b)) {
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
