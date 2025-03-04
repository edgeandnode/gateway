use std::{sync::Arc, time::Duration};

use ChainlinkPriceFeed::ChainlinkPriceFeedInstance;
use anyhow::ensure;
use ordered_float::NotNan;
use thegraph_core::alloy::{
    primitives::Address, providers::ProviderBuilder, sol, transports::http::Http,
};
use tokio::{
    sync::watch,
    time::{MissedTickBehavior, interval},
};
use url::Url;

sol!(
    #[sol(rpc)]
    ChainlinkPriceFeed,
    "src/contract_abis/ChainlinkPriceFeed.json",
);

// TODO: figure out how to erase this type
type Provider = thegraph_core::alloy::providers::RootProvider<Http<reqwest::Client>>;

pub async fn grt_per_usd(provider: Url) -> watch::Receiver<NotNan<f64>> {
    // https://data.chain.link/feeds/arbitrum/mainnet/grt-usd
    let chainlink_usd_per_grt: Address = "0x0F38D86FceF4955B705F35c9e41d1A16e0637c73"
        .parse()
        .unwrap();

    let provider = Arc::new(ProviderBuilder::new().on_http(provider));
    let usd_per_grt = ChainlinkPriceFeed::new(chainlink_usd_per_grt, provider);

    let (tx, mut rx) = watch::channel(NotNan::new(0.0).unwrap());
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;

            let usd_per_grt = match fetch_price(&usd_per_grt).await {
                Ok(usd_per_grt) => usd_per_grt,
                Err(exchange_rate_err) => {
                    tracing::error!(%exchange_rate_err);
                    continue;
                }
            };
            let grt_per_usd = match NotNan::new(usd_per_grt.recip()) {
                Ok(grt_per_usd) => grt_per_usd,
                Err(_) => continue,
            };
            tracing::info!(%grt_per_usd);
            tx.send(grt_per_usd).unwrap();
        }
    });

    rx.wait_for(|v| *v != 0.0).await.unwrap();
    rx
}

async fn fetch_price(
    contract: &ChainlinkPriceFeedInstance<Http<reqwest::Client>, Arc<Provider>>,
) -> anyhow::Result<NotNan<f64>> {
    let decimals: u8 = contract.decimals().call().await?._0;
    ensure!(decimals <= 18);
    let latest_answer: u128 = contract.latestAnswer().call().await?._0.try_into()?;
    ensure!(latest_answer > 0);
    Ok(NotNan::new(
        latest_answer as f64 * 10.0_f64.powi(-(decimals as i32)),
    )?)
}
