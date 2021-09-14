mod alchemy_client;
mod indexer_selection;
mod prelude;
mod query_engine;
mod sync_client;
mod ws_client;

use crate::{
    indexer_selection::SecretKey,
    prelude::*,
    query_engine::{BlockHead, Inputs, QueryEngine, Resolver, Response},
};
use async_trait::async_trait;
use indexer_selection::{IndexerQuery, UnresolvedBlock, UtilityConfig};
use std::error::Error;
use structopt::StructOpt;
use structopt_derive::StructOpt;
use tokio::time::Duration;
use tracing;
use tracing_subscriber;

#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(
        help = "URL of gateway agent syncing API",
        long = "--sync-agent",
        env = "SYNC_AGENT"
    )]
    sync_agent: String,
    #[structopt(
        help = "Ethereum WebSocket provider URLs, format: '<network>=<url>,...'",
        long = "--ethereum-ws",
        env = "ETHEREUM_WS",
        parse(try_from_str = "parse_networks")
    )]
    ethereum_ws: Vec<(String, String)>,
    #[structopt(help = "Format log output as JSON", long = "--log-json")]
    log_json: bool,
    #[structopt(
        long = "--indexer-selection-limit",
        env = "INDEXER_SELECTION_LIMIT",
        default_value = "5"
    )]
    indexer_selection_limit: usize,
}

fn parse_networks(arg: &str) -> Result<(String, String), String> {
    let kv = arg.split("=").collect::<Vec<&str>>();
    if kv.len() != 2 {
        return Err("networks syntax: <network>=<url>,...".into());
    }
    Ok((kv[0].into(), kv[1].into()))
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    init_tracing(opt.log_json);
    tracing::info!("Graph gateway starting...");
    tracing::trace!("{:#?}", opt);

    // TODO: set from mnemonic env var
    let signer_key =
        SecretKey::from_str("244226452948404D635166546A576E5A7234753778217A25432A462D4A614E64")
            .expect("Invalid mnemonic");

    let (input_writers, inputs) = Inputs::new();

    for (network, ws_url) in opt.ethereum_ws {
        alchemy_client::create(network, ws_url, input_writers.indexers.clone());
    }
    sync_client::create(
        opt.sync_agent,
        Duration::from_secs(30),
        signer_key,
        input_writers,
    );

    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    todo!("handle client queries");

    // let resolver = NetworkResolver {};

    // let query_engine = QueryEngine::new(
    //     query_engine::Config {
    //         indexer_selection_limit: opt.indexer_selection_limit,
    //         utility: UtilityConfig::default(),
    //     },
    //     resolver,
    //     inputs,
    // );
}

struct NetworkResolver {}

#[async_trait]
impl Resolver for NetworkResolver {
    async fn resolve_blocks(
        &self,
        network: &str,
        unresolved: &[UnresolvedBlock],
    ) -> Vec<BlockHead> {
        todo!()
    }

    async fn query_indexer(
        &self,
        query: &IndexerQuery,
    ) -> Result<Response<String>, Box<dyn Error>> {
        todo!()
    }
}
