mod alchemy_client;
mod indexer_selection;
mod prelude;
mod query_engine;
mod syncing_client;
mod ws_client;

use crate::{
    prelude::*,
    query_engine::{BlockHead, Inputs, QueryEngine, Resolver, Response},
};
use async_trait::async_trait;
use indexer_selection::{IndexerQuery, UnresolvedBlock, UtilityConfig};
use std::error::Error;
use structopt::StructOpt;
use structopt_derive::StructOpt;
use tracing;
use tracing_subscriber;

#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(
        help = "Ethereum WebSocket provider URLs, format: '<network>=<url>,...'",
        long = "--ethereum-ws",
        env = "ETHEREUM_WS",
        parse(try_from_str = "parse_networks")
    )]
    ethereum_ws: Vec<(String, String)>,
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
    init_tracing();
    tracing::info!("Graph gateway starting...");
    tracing::trace!("{:#?}", opt);

    let (input_writers, inputs) = Inputs::new();

    for (network, ws_url) in opt.ethereum_ws {
        alchemy_client::create(network, ws_url, input_writers.indexers.clone());
    }

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
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
