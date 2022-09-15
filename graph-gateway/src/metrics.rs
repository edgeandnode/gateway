use lazy_static::lazy_static;
use prometheus::{
    core::{MetricVec, MetricVecBuilder},
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge_vec, Histogram, HistogramTimer, HistogramVec, IntCounter, IntCounterVec,
    IntGaugeVec,
};

lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}

pub struct Metrics {
    pub client_query: ResponseMetricVecs,
    pub indexer_query: ResponseMetricVecs,
    pub network_subgraph: ResponseMetrics,
    pub collect_receipts: ResponseMetrics,
    pub partial_voucher: ResponseMetrics,
    pub voucher: ResponseMetrics,
    pub block_resolution: ResponseMetricVecs,
    pub block_cache_hit: IntCounterVec,
    pub block_cache_miss: IntCounterVec,
    pub chain_head: IntGaugeVec,
    pub indexer_selection_duration: HistogramVec,
}

impl Metrics {
    fn new() -> Self {
        Self {
            client_query: ResponseMetricVecs::new(
                "gw_client_query",
                "client query",
                &["deployment"],
            ),
            indexer_query: ResponseMetricVecs::new(
                "gw_indexer_query",
                "indexer query",
                &["deployment"],
            ),
            network_subgraph: ResponseMetrics::new("gw_network_subgraph", "network subgraph query"),
            collect_receipts: ResponseMetrics::new(
                "gw_collect_receipts",
                "collect-receipts request",
            ),
            partial_voucher: ResponseMetrics::new("gw_partial_voucher", "partial-voucher request"),
            voucher: ResponseMetrics::new("gw_voucher", "requests for voucher"),
            block_resolution: ResponseMetricVecs::new(
                "gw_block_resolution",
                "block requests",
                &["network"],
            ),
            block_cache_hit: register_int_counter_vec!(
                "gw_block_cache_hit",
                "block cache hit count",
                &["network"]
            )
            .unwrap(),
            block_cache_miss: register_int_counter_vec!(
                "gw_block_cache_miss",
                "block cache miss count",
                &["network"]
            )
            .unwrap(),
            chain_head: register_int_gauge_vec!(
                "gw_chain_head",
                "chain head block number",
                &["network"]
            )
            .unwrap(),
            indexer_selection_duration: register_histogram_vec!(
                "gw_indexer_selection_duration",
                "indexer selection duration",
                &["deployment"]
            )
            .unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct ResponseMetrics {
    pub ok: IntCounter,
    pub err: IntCounter,
    pub duration: Histogram,
}

impl ResponseMetrics {
    pub fn new(prefix: &str, description: &str) -> Self {
        let metrics = Self {
            ok: register_int_counter!(
                &format!("{}_ok", prefix),
                &format!("{} success count", description),
            )
            .unwrap(),
            err: register_int_counter!(
                &format!("{}_err", prefix),
                &format!("{} error count", description),
            )
            .unwrap(),
            duration: register_histogram!(
                &format!("{}_duration", prefix),
                &format!("{} duration", description),
            )
            .unwrap(),
        };
        metrics.ok.inc();
        metrics.err.inc();
        metrics
    }
}

#[derive(Clone)]
pub struct ResponseMetricVecs {
    pub ok: IntCounterVec,
    pub err: IntCounterVec,
    pub duration: HistogramVec,
}

impl ResponseMetricVecs {
    pub fn new(prefix: &str, description: &str, labels: &[&str]) -> Self {
        Self {
            ok: register_int_counter_vec!(
                &format!("{}_ok", prefix),
                &format!("{} success count", description),
                labels,
            )
            .unwrap(),
            err: register_int_counter_vec!(
                &format!("{}_err", prefix),
                &format!("{} error count", description),
                labels,
            )
            .unwrap(),
            duration: register_histogram_vec!(
                &format!("{}_duration", prefix),
                &format!("{} duration", description),
                labels,
            )
            .unwrap(),
        }
    }

    pub fn start_timer(&self, label_values: &[&str]) -> Option<HistogramTimer> {
        with_metric(&self.duration, label_values, |h| h.start_timer())
    }

    pub fn check<T, E>(&self, label_values: &[&str], result: &Result<T, E>) {
        match &result {
            Ok(_) => with_metric(&self.ok, label_values, |c| c.inc()),
            Err(_) => with_metric(&self.err, label_values, |c| c.inc()),
        };
    }
}

pub fn with_metric<T, F, B>(metric_vec: &MetricVec<B>, label_values: &[&str], f: F) -> Option<T>
where
    B: MetricVecBuilder,
    F: Fn(B::M) -> T,
{
    metric_vec
        .get_metric_with_label_values(label_values)
        .ok()
        .map(f)
}
