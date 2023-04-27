use ethers::{
    abi::Address,
    prelude::{abigen, Http},
    providers::Provider,
};
use prelude::{
    anyhow::{anyhow, ensure, Result},
    eventuals::{self, EventualExt},
    tokio::sync::Mutex,
    tracing, Duration, Eventual, EventualWriter, UDecimal, Url, USD,
};
use std::sync::Arc;

abigen!(
    UniswapV3Pool,
    "graph-gateway/src/contract_abis/UniswapV3Pool.json",
    event_derives(serde::Deserialize, serde::Serialize);
);

pub async fn usd_to_grt(provider: Url) -> Result<Eventual<USD>> {
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
    let writer: &'static Mutex<EventualWriter<USD>> = Box::leak(Box::new(Mutex::new(writer)));
    eventuals::timer(Duration::from_secs(60))
        .pipe_async(move |_| async {
            match fetch_usd_to_grt(pool).await {
                Err(usd_to_grt_err) => tracing::error!(%usd_to_grt_err),
                Ok(usd_to_grt) => {
                    tracing::info!(%usd_to_grt);
                    writer.lock().await.write(usd_to_grt);
                }
            };
        })
        .forever();
    Ok(reader)
}

async fn fetch_usd_to_grt(pool: &UniswapV3Pool<Provider<Http>>) -> Result<USD> {
    const GRT_DECIMALS: u8 = 18;
    const USDC_DECIMALS: u8 = 6;
    // https://docs.uniswap.org/contracts/v3/reference/core/interfaces/pool/IUniswapV3PoolDerivedState#observe
    // token1/token0 -> GRT/USDC
    let twap_seconds: u32 = 3600;
    let (tick_cumulatives, _) = pool.observe(vec![twap_seconds, 0]).await?;
    let tick = (tick_cumulatives[1] - tick_cumulatives[0]) / twap_seconds as i64;
    let price = UDecimal::<0>::try_from(1.0001_f64.powi(tick as i32) as u128)
        .map_err(|err| anyhow!(err))?
        .shift::<{ GRT_DECIMALS - USDC_DECIMALS }>();
    Ok(price.change_precision())
}
