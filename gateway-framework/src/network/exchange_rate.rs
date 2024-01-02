use std::sync::Arc;
use std::time::Duration;

use anyhow::ensure;
use ethers::{
    abi::Address,
    prelude::{abigen, Http},
    providers::Provider,
};
use eventuals::{Eventual, EventualExt, EventualWriter};
use indexer_selection::NotNan;
use tokio::sync::Mutex;
use toolshed::url::Url;

use gateway_common::types::{UDecimal18, GRT};

abigen!(
    ChainlinkPriceFeed,
    "gateway-framework/src/contract_abis/ChainlinkPriceFeed.json",
    event_derives(serde::Deserialize, serde::Serialize);
);

pub async fn grt_per_usd(provider: Url) -> anyhow::Result<Eventual<GRT>> {
    // https://data.chain.link/ethereum/mainnet/crypto-eth/grt-eth
    let chainlink_eth_per_grt: Address = "0x17d054ecac33d91f7340645341efb5de9009f1c1"
        .parse()
        .unwrap();
    // https://data.chain.link/ethereum/mainnet/crypto-usd/eth-usd
    let chainlink_usd_per_eth: Address = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
        .parse()
        .unwrap();

    let provider = Arc::new(Provider::<Http>::try_from(provider.to_string()).unwrap());
    let eth_per_grt: &'static ChainlinkPriceFeed<Provider<Http>> = Box::leak(Box::new(
        ChainlinkPriceFeed::new(chainlink_eth_per_grt, provider.clone()),
    ));
    let usd_per_eth: &'static ChainlinkPriceFeed<Provider<Http>> = Box::leak(Box::new(
        ChainlinkPriceFeed::new(chainlink_usd_per_eth, provider),
    ));

    let (writer, reader) = Eventual::new();
    let writer: &'static Mutex<EventualWriter<GRT>> = Box::leak(Box::new(Mutex::new(writer)));
    eventuals::timer(Duration::from_secs(60))
        .pipe_async(move |_| async {
            let eth_per_grt = match fetch_price(eth_per_grt).await {
                Ok(eth_per_grt) => eth_per_grt,
                Err(eth_per_grt_err) => {
                    tracing::error!(%eth_per_grt_err);
                    return;
                }
            };
            let usd_per_eth = match fetch_price(usd_per_eth).await {
                Ok(usd_per_eth) => usd_per_eth,
                Err(usd_per_eth_err) => {
                    tracing::error!(%usd_per_eth_err);
                    return;
                }
            };
            let usd_per_grt = eth_per_grt * usd_per_eth;
            let grt_per_usd = match UDecimal18::try_from(usd_per_grt.recip()) {
                Ok(grt_per_usd) => GRT(grt_per_usd),
                Err(grt_per_usd_err) => {
                    tracing::error!(%grt_per_usd_err);
                    return;
                }
            };
            tracing::info!(grt_per_usd = %grt_per_usd.0);
            writer.lock().await.write(grt_per_usd);
        })
        .forever();
    Ok(reader)
}

async fn fetch_price(contract: &ChainlinkPriceFeed<Provider<Http>>) -> anyhow::Result<NotNan<f64>> {
    let decimals: u8 = contract.decimals().await?;
    ensure!(decimals <= 18);
    let latest_answer: u128 = contract.latest_answer().await?.try_into()?;
    ensure!(latest_answer > 0);
    Ok(NotNan::new(
        latest_answer as f64 * 10.0_f64.powi(-(decimals as i32)),
    )?)
}
