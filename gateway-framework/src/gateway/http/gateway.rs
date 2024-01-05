use std::collections::HashMap;
use std::time::Duration;

use eventuals::{Eventual, EventualExt};
use tokio::spawn;
use toolshed::buffer_queue::QueueWriter;
use toolshed::{buffer_queue, double_buffer};

use gateway_common::types::GRT;
use indexer_selection::actor::Update;

use crate::chains::{ethereum, BlockCache};
use crate::config::ExchangeRateProvider;
use crate::gateway::http::GatewayConfig;
use crate::geoip::GeoIP;
use crate::network::exchange_rate;

pub trait GatewayImpl {}

pub struct Gateway {}

pub struct GatewayOptions<G>
where
    G: GatewayImpl + Sync + Send + 'static,
{
    pub gateway_impl: G,
    pub config: GatewayConfig,
}

impl Gateway {
    pub async fn run<G>(options: GatewayOptions<G>)
    where
        G: GatewayImpl + Sync + Send + 'static,
    {
        let GatewayOptions { config, .. } = options;

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
