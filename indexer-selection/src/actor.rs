use crate::{IndexerErrorObservation, IndexerInfo, Indexing, IndexingStatus, State};
use lazy_static::lazy_static;
use prelude::*;
use prelude::{
    buffer_queue::{Event, QueueReader},
    double_buffer::DoubleBufferWriter,
};
use prometheus;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    select,
    time::{sleep_until, Duration, Instant},
};

#[derive(Debug)]
pub enum Update {
    USDToGRTConversion(GRT),
    SlashingPercentage(PPM),
    Indexers(HashMap<Address, IndexerUpdate>),
    QueryObservation {
        indexing: Indexing,
        duration: Duration,
        result: Result<(), IndexerErrorObservation>,
    },
    Penalty {
        indexing: Indexing,
        weight: u8,
    },
}

#[derive(Debug)]
pub struct IndexerUpdate {
    pub info: Arc<IndexerInfo>,
    pub indexings: HashMap<SubgraphDeploymentID, IndexingStatus>,
}

pub async fn process_updates(
    mut writer: DoubleBufferWriter<State>,
    mut events: QueueReader<Update>,
) {
    let mut event_buffer = Vec::new();
    let mut next_decay = Instant::now() + Duration::from_secs(60);

    loop {
        select! {
            _ = sleep_until(next_decay) => {
                next_decay = Instant::now() + Duration::from_secs(60);
                writer.update(|indexers| indexers.decay()).await;
            },
            _ = events.read(&mut event_buffer) => {
                tracing::trace!(isa_update_queue_depth = event_buffer.len());
                METRICS.isa_update_queue_depth.set(event_buffer.len() as i64);
                writer
                    .update(|state| {
                        for update in event_buffer.iter() {
                            if let Event::Update(update) = update {
                                apply_state_update(state, update);
                            }
                        }
                    })
                    .await;
                let mut waiters = 0;
                for update in event_buffer.iter() {
                    if let Event::Flush(notify) = update {
                        notify.notify_one();
                        waiters += 1;
                    }
                }
                METRICS.isa_update_queue_waiters.set(waiters);
                event_buffer.clear();
            },
        }
    }
}

pub fn apply_state_update(state: &mut State, update: &Update) {
    match update {
        Update::USDToGRTConversion(usd_to_grt) => {
            tracing::info!(%usd_to_grt);
            state.network_params.usd_to_grt_conversion = Some(*usd_to_grt);
        }
        Update::SlashingPercentage(slashing_percentage) => {
            tracing::info!(%slashing_percentage);
            state.network_params.slashing_percentage = Some(*slashing_percentage);
        }
        Update::Indexers(indexers) => {
            for (indexer, indexer_update) in indexers {
                state.indexers.insert(*indexer, indexer_update.info.clone());
                for (deployment, status) in &indexer_update.indexings {
                    let indexing = Indexing {
                        indexer: *indexer,
                        deployment: *deployment,
                    };
                    state.insert_indexing(indexing, status.clone());
                }
            }
            state.indexers.increment_epoch();
            state.indexings.increment_epoch();
        }
        Update::QueryObservation {
            indexing,
            duration,
            result,
        } => {
            state.observe_query(indexing, *duration, *result);
        }
        Update::Penalty { indexing, weight } => state.penalize(indexing, *weight),
    }
}

lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}

pub struct Metrics {
    pub isa_update_queue_depth: prometheus::IntGauge,
    pub isa_update_queue_waiters: prometheus::IntGauge,
}

impl Metrics {
    fn new() -> Self {
        Self {
            isa_update_queue_depth: prometheus::register_int_gauge!(
                "gw_isa_update_queue_depth",
                "ISA update queue depth",
            )
            .unwrap(),
            isa_update_queue_waiters: prometheus::register_int_gauge!(
                "gw_isa_update_queue_waiters",
                "ISA update queue waiters",
            )
            .unwrap(),
        }
    }
}
