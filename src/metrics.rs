use lazy_static::lazy_static;
use prometheus::{
    core::{MetricVec, MetricVecBuilder},
    register_gauge, register_histogram, register_histogram_vec, register_int_counter,
    register_int_counter_vec, register_int_gauge_vec, Gauge, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGaugeVec,
};

lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}

pub struct Metrics {
    pub client_query: ResponseMetrics,
    pub avg_query_fees: Gauge,
    pub indexer_query: ResponseMetricVecs,
    pub blocks_per_minute: IntGaugeVec,
}

impl Metrics {
    fn new() -> Self {
        Self {
            client_query: ResponseMetrics::new("gw_client_query", "client query"),
            avg_query_fees: register_gauge!(
                "gw_avg_query_fees",
                "average indexer fees per query, in USD"
            )
            .unwrap(),
            indexer_query: ResponseMetricVecs::new(
                "gw_indexer_query",
                "indexer query",
                &["deployment", "indexer"],
            ),
            blocks_per_minute: register_int_gauge_vec!(
                "gw_blocks_per_minute",
                "chain blocks per minute",
                &["chain"]
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
                &format!("{prefix}_ok"),
                &format!("{description} success count"),
            )
            .unwrap(),
            err: register_int_counter!(
                &format!("{prefix}_err"),
                &format!("{description} error count"),
            )
            .unwrap(),
            duration: register_histogram!(
                &format!("{prefix}_duration"),
                &format!("{description} duration"),
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
                &format!("{prefix}_ok"),
                &format!("{description} success count"),
                labels,
            )
            .unwrap(),
            err: register_int_counter_vec!(
                &format!("{prefix}_err"),
                &format!("{description} error count"),
                labels,
            )
            .unwrap(),
            duration: register_histogram_vec!(
                &format!("{prefix}_duration"),
                &format!("{description} duration"),
                labels,
            )
            .unwrap(),
        }
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
