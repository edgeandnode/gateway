use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::U256;
use alloy_sol_types::Eip712Domain;
use eventuals::{Eventual, EventualExt};
use thegraph::client as subgraph_client;
use thegraph::types::{attestation, DeploymentId};
use tokio::spawn;
use toolshed::buffer_queue::QueueWriter;
use toolshed::{buffer_queue, double_buffer};

use gateway_common::types::{Indexing, GRT};
use indexer_selection::actor::Update;

use crate::chains::{ethereum, BlockCache};
use crate::config::ExchangeRateProvider;
use crate::gateway::http::GatewayConfig;
use crate::geoip::GeoIP;
use crate::indexers::status::IndexingStatus;
use crate::ipfs;
use crate::network::datasets::{Dataset, Deployment};
use crate::network::{exchange_rate, network_subgraph};

pub trait GatewayImpl {}

pub struct Gateway {}

pub struct GatewayOptions<G>
where
    G: GatewayImpl + Sync + Send + 'static,
{
    pub gateway_impl: G,
    pub datasets: Eventual<Ptr<Vec<Dataset>>>,
    pub config: GatewayConfig,
}

impl Gateway {
    pub async fn run<G>(options: GatewayOptions<G>)
    where
        G: GatewayImpl + Sync + Send + 'static,
    {
        let GatewayOptions {
            config, datasets, ..
        } = options;

        let (isa_state, isa_writer) = double_buffer!(indexer_selection::State::default());

        // Start the actor to manage ISA updates
        let (update_writer, update_reader) = buffer_queue::pair();
        spawn(async move {
            indexer_selection::actor::process_updates(isa_writer, update_reader).await;
            tracing::error!("ISA actor stopped");
        });

        let geoip = config
            .geoip_database
            .filter(|_| !config.geoip_blocked_countries.is_empty())
            .map(|db| GeoIP::new(db, config.geoip_blocked_countries).unwrap());

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();

        let block_caches: HashMap<String, &'static BlockCache> = config
            .chains
            .into_iter()
            .flat_map(|chain| {
                let cache: &'static BlockCache =
                    Box::leak(Box::new(BlockCache::new::<ethereum::Client>(chain.clone())));
                chain.names.into_iter().map(move |alias| (alias, cache))
            })
            .collect();
        let block_caches: &'static HashMap<String, &'static BlockCache> =
            Box::leak(Box::new(block_caches));

        let grt_per_usd: Eventual<GRT> = match config.exchange_rate_provider {
            ExchangeRateProvider::Fixed(grt_per_usd) => Eventual::from_value(GRT(grt_per_usd)),
            ExchangeRateProvider::Rpc(url) => exchange_rate::grt_per_usd(url).await.unwrap(),
        };
        update_from_eventual(
            grt_per_usd.clone(),
            update_writer.clone(),
            Update::GRTPerUSD,
        );

        let network_subgraph_client =
            subgraph_client::Client::new(http_client.clone(), config.network_subgraph.clone());
        let network_subgraph_data =
            network_subgraph::Client::create(network_subgraph_client, config.l2_gateway.is_some())
                .await
                .unwrap();

        update_writer
            .write(Update::SlashingPercentage(
                network_subgraph_data.network_params.slashing_percentage,
            ))
            .unwrap();

        let attestation_domain: &'static Eip712Domain =
            Box::leak(Box::new(attestation::eip712_domain(
                U256::from_str_radix(&config.attestations.chain_id, 10)
                    .expect("failed to parse attestation domain chain_id"),
                config.attestations.dispute_manager,
            )));

        let ipfs = ipfs::Client::new(http_client.clone(), config.ipfs, 50);

        // TODO: How do we incorporate the indexings blocklist from the subgraph gateway here?

        // Create a lookup table for deployments, keyed by their ID (which is also their IPFS hash).
        let deployments = datasets.clone().map(|dataset| async move {
            dataset
                .values()
                .flat_map(|dataset| &dataset.deployments)
                .map(|deployment| (deployment.id, deployment.clone()))
                .collect::<HashMap<DeploymentId, Arc<Deployment>>>()
                .into()
        });

        // Create a lookup table for indexers, keyed by their ID (which is also their address).
        let indexers = subgraphs.clone().map(|subgraphs| async move {
            subgraphs
                .values()
                .flat_map(|subgraph| &subgraph.deployments)
                .flat_map(|deployment| &deployment.indexers)
                .map(|indexer| (indexer.id, indexer.clone()))
                .collect::<HashMap<Address, Arc<Indexer>>>()
                .into()
        });

        // Return only after eventuals have values, to avoid serving client queries prematurely.
        if deployments.value().await.is_err() || indexers.value().await.is_err() {
            panic!("Failed to await Graph network topology");
        }
    }
}

fn update_from_eventual<V, F>(eventual: Eventual<V>, writer: QueueWriter<Update>, f: F)
where
    V: eventuals::Value,
    F: 'static + Send + Fn(V) -> Update,
{
    eventual
        .pipe(move |v| {
            let _ = writer.write(f(v));
        })
        .forever();
}
