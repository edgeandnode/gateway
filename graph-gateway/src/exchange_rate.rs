use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::U256;
use anyhow::ensure;
use ethers::{
    abi::Address,
    prelude::{abigen, Http},
    providers::Provider,
};
use eventuals::{Eventual, EventualExt, EventualWriter};
use tokio::sync::Mutex;
use toolshed::url::Url;

use gateway_common::types::{UDecimal18, GRT};

abigen!(
    UniswapV3Pool,
    "graph-gateway/src/contract_abis/UniswapV3Pool.json",
    event_derives(serde::Deserialize, serde::Serialize);
);

pub async fn grt_per_usd(provider: Url) -> anyhow::Result<Eventual<GRT>> {
    // https://info.uniswap.org/#/pools/0x4d1fb02fa84eda35881902e8e0fdacc3a873398b
    let uniswap_v3_pool: Address = "0x4d1Fb02fa84EdA35881902e8E0fdacC3a873398B"
        .parse()
        .unwrap();
    let provider = Arc::new(Provider::<Http>::try_from(provider.to_string()).unwrap());
    let pool: &'static UniswapV3Pool<Provider<Http>> =
        Box::leak(Box::new(UniswapV3Pool::new(uniswap_v3_pool, provider)));

    let usdc: Address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        .parse()
        .unwrap();
    let grt: Address = "0xc944E90C64B2c07662A292be6244BDf05Cda44a7"
        .parse()
        .unwrap();
    ensure!(pool.token_0().await.unwrap() == usdc);
    ensure!(pool.token_1().await.unwrap() == grt);

    let (writer, reader) = Eventual::new();
    let writer: &'static Mutex<EventualWriter<GRT>> = Box::leak(Box::new(Mutex::new(writer)));
    eventuals::timer(Duration::from_secs(60))
        .pipe_async(move |_| async {
            match fetch_grt_per_usd(pool).await {
                Err(grt_per_usd_err) => tracing::error!(%grt_per_usd_err),
                Ok(grt_per_usd) => {
                    tracing::info!(grt_per_usd = %grt_per_usd.0);
                    writer.lock().await.write(grt_per_usd);
                }
            };
        })
        .forever();
    Ok(reader)
}

async fn fetch_grt_per_usd(pool: &UniswapV3Pool<Provider<Http>>) -> anyhow::Result<GRT> {
    const GRT_DECIMALS: u32 = 18;
    const USDC_DECIMALS: u32 = 6;
    // https://docs.uniswap.org/contracts/v3/reference/core/interfaces/pool/IUniswapV3PoolDerivedState#observe
    // token1/token0 -> GRT/USDC
    let twap_seconds: u32 = 60 * 20;
    let (tick_cumulatives, _) = pool.observe(vec![twap_seconds, 0]).await?;
    ensure!(tick_cumulatives.len() == 2);
    let tick = (tick_cumulatives[1] - tick_cumulatives[0]) / twap_seconds as i64;
    let price = U256::try_from(1.0001_f64.powi(tick as i32)).unwrap();
    let shift = U256::from(10_u128.pow(18 - (GRT_DECIMALS - USDC_DECIMALS)));
    let price = UDecimal18::from_raw_u256(price * shift);
    Ok(GRT(price))
}
