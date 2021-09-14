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
use std::{collections::HashMap, error::Error};
use structopt::StructOpt;
use structopt_derive::StructOpt;
use tokio::{
    sync::{mpsc, oneshot},
    time::Duration,
};

#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(
        help = "URL of gateway agent syncing API",
        long = "--sync-agent",
        env = "SYNC_AGENT"
    )]
    sync_agent: String,
    #[structopt(
        help = "Ethereum provider URLs, format: '<network>=<url>,...'\ne.g. rinkeby=eth-rinkeby.alchemyapi.io/v2/<api-key>",
        long = "--ethereum-providers",
        env = "ETHEREUM_PROVIDERS",
        parse(try_from_str = "parse_networks")
    )]
    ethereum_proviers: Vec<(String, String)>,
    #[structopt(help = "Format log output as JSON", long = "--log-json")]
    log_json: bool,
    #[structopt(
        long = "--indexer-selection-retry-limit",
        env = "INDEXER_SELECTION_LIMIT",
        default_value = "5"
    )]
    indexer_selection_retry_limit: usize,
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
    let block_resolvers = opt
        .ethereum_proviers
        .into_iter()
        .map(|(network, ws_url)| {
            (
                network.clone(),
                alchemy_client::create(network, ws_url, input_writers.indexers.clone()),
            )
        })
        .collect::<HashMap<String, mpsc::Sender<alchemy_client::Request>>>();
    sync_client::create(
        opt.sync_agent,
        Duration::from_secs(30),
        signer_key,
        input_writers,
    );
    // TODO: argument for timeout
    let query_engine = QueryEngine::new(
        query_engine::Config {
            indexer_selection_retry_limit: opt.indexer_selection_retry_limit,
            utility: UtilityConfig::default(),
        },
        NetworkResolver {
            block_resolvers,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
        },
        inputs,
    );
    panic!("TODO: handle queries");
}

struct NetworkResolver {
    block_resolvers: HashMap<String, mpsc::Sender<alchemy_client::Request>>,
    client: reqwest::Client,
}

#[async_trait]
impl Resolver for NetworkResolver {
    #[tracing::instrument(skip(self, network, unresolved))]
    async fn resolve_blocks(
        &self,
        network: &str,
        unresolved: &[UnresolvedBlock],
    ) -> Vec<BlockHead> {
        use alchemy_client::Request;
        let mut resolved_blocks = Vec::new();
        let resolver = match self.block_resolvers.get(network) {
            Some(resolver) => resolver,
            None => {
                tracing::error!(missing_network = network);
                return resolved_blocks;
            }
        };
        for unresolved_block in unresolved {
            let (sender, receiver) = oneshot::channel();
            if let Err(_) = resolver
                .send(Request::Block(unresolved_block.clone(), sender))
                .await
            {
                tracing::error!("block resolver connection closed");
                return resolved_blocks;
            }
            match receiver.await {
                Ok(resolved) => resolved_blocks.push(resolved),
                Err(_) => {
                    tracing::error!("block resolver connection closed");
                    return resolved_blocks;
                }
            };
        }
        resolved_blocks
    }

    async fn query_indexer(
        &self,
        query: &IndexerQuery,
    ) -> Result<Response<String>, Box<dyn Error>> {
        let receipt = hex::encode(&query.receipt[0..(query.receipt.len() - 64)]);
        self.client
            .post(format!(
                "{}/subgraphs/id/{:?}",
                query.url, query.indexing.subgraph
            ))
            .header("X-Graph-Payment", &receipt)
            .header("Scalar-Receipt", &receipt)
            .body(query.query.clone())
            .send()
            .await?
            .json::<Response<String>>()
            .await
            .map_err(|err| err.into())
    }
}
