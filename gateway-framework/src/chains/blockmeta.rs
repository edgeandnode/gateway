//! StreamingFast Blockmeta client.

use alloy_primitives::{BlockHash, BlockNumber};
use std::time::Duration;

use custom_debug::CustomDebug;
use thegraph_core::types::BlockPointer;
use tokio::sync::mpsc;
use tokio::time::interval;
use tonic::transport::Uri;
use tracing::Instrument;

use crate::chains::{BlockHead, ClientMsg, UnresolvedBlock};
use crate::metrics::METRICS;

use self::rpc_client::{AuthChannel, Block, BlockmetaClient};

pub mod rpc_client;

/// Streamingfast Blockmeta block resolution client configuration.
#[derive(Clone, CustomDebug)]
pub struct Config {
    /// The first name is used in logs, the others are aliases also supported in subgraph manifests.
    pub names: Vec<String>,
    /// The URI of the blockmeta service.
    pub uri: Uri,
    /// The authentication token for the blockmeta service.
    #[debug(skip)]
    pub auth: String,
}

/// Streamingfast Blockmeta block resolution client
pub struct Client {
    /// Chain configuration
    conf: Config,
    /// RPC client
    rpc_client: BlockmetaClient<AuthChannel>,
    /// Channel to send resolved blocks to upstream
    notify: mpsc::UnboundedSender<ClientMsg>,
}

impl super::Client for Client {
    type Config = Config;

    fn chain_name(config: &Self::Config) -> &str {
        &config.names[0]
    }

    fn poll_interval() -> Duration {
        Duration::from_secs(2)
    }

    /// Create a new Ethereum block resolution client and
    fn create(
        conf: Config,
        notify: mpsc::UnboundedSender<ClientMsg>,
    ) -> mpsc::UnboundedSender<UnresolvedBlock> {
        let _ = tracing::info_span!("blockmeta_client", chain = %conf.names[0]).entered();

        let (unresolved_tx, mut unresolved_rx) = mpsc::unbounded_channel();

        let chain_uri = conf.uri.clone();
        let rpc_client = BlockmetaClient::new_with_auth(chain_uri, &conf.auth);
        tokio::spawn(
            async move {
                let mut client = Self {
                    conf,
                    rpc_client,
                    notify,
                };

                let mut poll_timer = interval(Self::poll_interval());
                loop {
                    // Periodically fetch the latest block, and also fetch any unresolved blocks
                    tokio::select! {
                        _ = poll_timer.tick() => {
                            client.spawn_fetch_block(None);
                        },
                        Some(unresolved) = unresolved_rx.recv() => {
                            client.spawn_fetch_block(Some(unresolved));
                        },
                        else => break,
                    }
                }
                tracing::error!("Blockmeta client exit");
            }
            .in_current_span(),
        );

        unresolved_tx
    }
}

impl Client {
    /// Spawn a task to fetch a block (or the current head) and send the result
    /// to the notify channel
    fn spawn_fetch_block(&mut self, unresolved: Option<UnresolvedBlock>) {
        let client = self.rpc_client.clone();
        let chain = self.conf.names[0].clone();
        let notify = self.notify.clone();

        tokio::spawn(async move {
            let timer = METRICS.block_resolution.start_timer(&[&chain]);
            let result = Self::fetch_block(client, &unresolved).await;
            drop(timer);

            METRICS.block_resolution.check(&[&chain], &result);

            let response = match result {
                Ok(head) => match &unresolved {
                    Some(_) => ClientMsg::Block(head.block),
                    None => ClientMsg::Head(head),
                },
                Err(fetch_block_err) => {
                    if unresolved.is_none() {
                        tracing::error!(%chain, %fetch_block_err);
                    } else {
                        tracing::warn!(%chain, ?unresolved, %fetch_block_err);
                    }
                    match unresolved {
                        Some(unresolved) => ClientMsg::Err(unresolved),
                        None => return,
                    }
                }
            };

            let _ = notify.send(response);
        });
    }

    /// Fetch a block (or the current head) using the StreamingFast Blockmeta service.
    async fn fetch_block(
        mut client: BlockmetaClient<AuthChannel>,
        unresolved: &Option<UnresolvedBlock>,
    ) -> anyhow::Result<BlockHead> {
        let res: Option<Block> = match &unresolved {
            Some(UnresolvedBlock::WithHash(hash)) => {
                // Resolve the block by hash
                client.get_block_by_hash(*hash).await?
            }
            Some(UnresolvedBlock::WithNumber(number)) => {
                // Resolve the block by number
                client.get_block_by_number(*number).await?
            }
            None => {
                // Resolve the latest block
                client.get_latest_block().await?
            }
        };

        // Check if the block was found
        match res {
            None => Err(anyhow::anyhow!("block not found")),
            Some(bl) => {
                let block_hash = bl
                    .id
                    .parse::<BlockHash>()
                    .map_err(|err| anyhow::anyhow!("invalid block hash format: {}", err))?;
                let block_number = bl.num as BlockNumber;

                Ok(BlockHead {
                    block: BlockPointer {
                        hash: block_hash,
                        number: block_number,
                    },
                    uncles: vec![],
                })
            }
        }
    }
}
