use crate::{IndexerInfo, Indexing, IndexingStatus, State};
use prelude::*;
use prelude::{buffer_queue::QueueReader, double_buffer::DoubleBufferWriter};
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

#[derive(Debug)]
pub enum IndexerErrorObservation {
    Timeout,
    IndexingBehind {
        latest_query_block: u64,
        latest_block: u64,
    },
    Other,
}

impl IndexerErrorObservation {
    fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout => true,
            _ => false,
        }
    }
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
                writer
                    .update(|state| {
                        for update in event_buffer.iter() {
                            apply_state_update(state, update);
                        }
                    })
                    .await;
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
                state
                    .indexers
                    .insert(indexer.clone(), indexer_update.info.clone());
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
        } => match result {
            Ok(()) => state.observe_successful_query(indexing, *duration),
            Err(error) => {
                state.observe_failed_query(indexing, *duration, error.is_timeout());
                if let IndexerErrorObservation::IndexingBehind {
                    latest_query_block,
                    latest_block,
                } = error
                {
                    state.observe_indexing_behind(indexing, *latest_query_block, *latest_block);
                }
            }
        },
        Update::Penalty { indexing, weight } => state.penalize(indexing, *weight),
    }
}
