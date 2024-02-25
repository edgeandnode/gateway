//! Ethereum RPC block resolution client.

use std::fmt::Display;
use std::time::Duration;

use custom_debug::CustomDebug;
use thegraph::types::BlockPointer;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::Instrument;
use url::Url;

use crate::metrics::METRICS;

use super::{BlockHead, ClientMsg, UnresolvedBlock};

use self::rpc_client::{Block, BlockByNumberParam, EthRpcClient};

mod json_rpc;
pub mod rpc_client;

/// The Ethereum block resolution client configuration.
#[derive(Clone, CustomDebug)]
pub struct Config {
    /// Chain names.
    ///
    /// The first name is used in logs, the others are aliases also supported in subgraph manifests.
    pub names: Vec<String>,

    /// The RPC URL for the chain.
    #[debug(with = "Display::fmt")]
    pub url: Url,
}

/// Ethereum block resolution client
pub struct Client {
    /// Chain configuration
    chain: Config,
    /// Ethereum RPC client
    rpc_client: EthRpcClient,
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
        conf: Self::Config,
        notify: mpsc::UnboundedSender<ClientMsg>,
    ) -> mpsc::UnboundedSender<UnresolvedBlock> {
        let _span = tracing::info_span!("ethereum_client", chain = %conf.names[0]).entered();

        let (unresolved_tx, mut unresolved_rx) = mpsc::unbounded_channel();

        let rpc_client = EthRpcClient::new(reqwest::Client::new(), conf.url.clone());
        tokio::spawn(
            async move {
                let mut client = Self {
                    chain: conf,
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
                tracing::error!("Ethereum client exit");
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
        let chain = self.chain.names[0].clone();
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

    /// Fetch a block (or the current head) using the Ethereum RPC client
    async fn fetch_block(
        client: EthRpcClient,
        unresolved: &Option<UnresolvedBlock>,
    ) -> anyhow::Result<BlockHead> {
        let res: Option<Block> = match &unresolved {
            Some(UnresolvedBlock::WithHash(hash)) => {
                // Resolve the block by hash
                client.get_block_by_hash(*hash).await?
            }
            Some(UnresolvedBlock::WithNumber(number)) => {
                // Resolve the block by number
                client
                    .get_block_by_number(BlockByNumberParam::Number(*number))
                    .await?
            }
            None => {
                // Resolve the latest block
                client
                    .get_block_by_number(BlockByNumberParam::Latest)
                    .await?
            }
        };

        // Check if the block was found and if it was not a pending block
        match res {
            None => Err(anyhow::anyhow!("block not found")),
            Some(bl) => {
                // If the block hash or the block number is missing, then it is a pending block
                let block_hash = bl.hash.ok_or_else(|| anyhow::anyhow!("pending block"))?;
                let block_number = bl.number.ok_or_else(|| anyhow::anyhow!("pending block"))?;

                Ok(BlockHead {
                    block: BlockPointer {
                        hash: block_hash,
                        number: block_number,
                    },
                    uncles: bl.uncles,
                })
            }
        }
    }
}
