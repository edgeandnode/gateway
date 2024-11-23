use std::{sync::Arc, time::Duration};

use alloy::{primitives::Address, providers::ProviderBuilder, sol, transports::http::Http};
use anyhow::ensure;
use ordered_float::NotNan;
use tokio::{
    sync::watch,
    time::{interval, MissedTickBehavior},
};
use url::Url;
use ChainlinkPriceFeed::ChainlinkPriceFeedInstance;

sol!(
    #[sol(rpc)]
    ChainlinkPriceFeed,
    "src/contract_abis/ChainlinkPriceFeed.json",
);

// TODO: figure out how to erase this type
type Provider = alloy::providers::RootProvider<Http<reqwest::Client>>;

pub async fn grt_per_usd(provider: Url) -> anyhow::Result<watch::Receiver<NotNan<f64>>> {
    // https://data.chain.link/ethereum/mainnet/crypto-eth/grt-eth
    let chainlink_eth_per_grt: Address = "0x17d054ecac33d91f7340645341efb5de9009f1c1"
        .parse()
        .unwrap();
    // https://data.chain.link/ethereum/mainnet/crypto-usd/eth-usd
    let chainlink_usd_per_eth: Address = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
        .parse()
        .unwrap();

    let provider = Arc::new(ProviderBuilder::new().on_http(provider));
    let eth_per_grt = ChainlinkPriceFeed::new(chainlink_eth_per_grt, provider.clone());
    let usd_per_eth = ChainlinkPriceFeed::new(chainlink_usd_per_eth, provider);

    let (tx, mut rx) = watch::channel(NotNan::new(0.0).unwrap());
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            let eth_per_grt = match fetch_price(&eth_per_grt).await {
                Ok(eth_per_grt) => eth_per_grt,
                Err(eth_per_grt_err) => {
                    tracing::error!(%eth_per_grt_err);
                    return;
                }
            };
            let usd_per_eth = match fetch_price(&usd_per_eth).await {
                Ok(usd_per_eth) => usd_per_eth,
                Err(usd_per_eth_err) => {
                    tracing::error!(%usd_per_eth_err);
                    return;
                }
            };
            let grt_per_usd = NotNan::new((eth_per_grt * usd_per_eth).recip()).unwrap();
            tracing::info!(%grt_per_usd);
            if let Err(grt_per_usd_send_err) = tx.send(grt_per_usd) {
                tracing::error!(%grt_per_usd_send_err);
            }
        }
    });

    let _ = rx.wait_for(|v| *v != 0.0).await;
    Ok(rx)
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
