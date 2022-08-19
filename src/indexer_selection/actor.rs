use crate::{
    indexer_selection::{BlockRequirements, IndexerInfo, Indexing, IndexingStatus, State},
    prelude::*,
    utils::{buffer_queue::QueueReader, double_buffer::DoubleBufferWriter},
};
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
        refreshed_requirements: BlockRequirements,
        latest: u64,
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
                process_events(&mut event_buffer, &mut writer).await;
            },
        }
    }
}

#[cfg(test)]
pub async fn test_process_updates(
    writer: &mut DoubleBufferWriter<State>,
    events: &mut QueueReader<Update>,
) {
    let mut event_buffer = Vec::new();
    events.try_read(&mut event_buffer);
    process_events(&mut event_buffer, writer).await;
}

async fn process_events(event_buffer: &mut Vec<Update>, writer: &mut DoubleBufferWriter<State>) {
    writer
        .update(|isa| {
            for event in event_buffer.iter() {
                match event {
                    Update::USDToGRTConversion(usd_to_grt) => {
                        isa.network_params.usd_to_grt_conversion = Some(*usd_to_grt);
                    }
                    Update::SlashingPercentage(slashing_percentage) => {
                        isa.network_params.slashing_percentage = Some(*slashing_percentage);
                    }
                    Update::Indexers(indexers) => {
                        for (indexer, indexer_update) in indexers {
                            isa.indexers
                                .insert(indexer.clone(), indexer_update.info.clone());
                            for (deployment, status) in &indexer_update.indexings {
                                let indexing = Indexing {
                                    indexer: *indexer,
                                    deployment: *deployment,
                                };
                                isa.insert_indexing(indexing, status.clone());
                            }
                        }
                        isa.indexers.increment_epoch();
                        isa.indexings.increment_epoch();
                    }
                    Update::QueryObservation {
                        indexing,
                        duration,
                        result,
                    } => match result {
                        Ok(()) => isa.observe_successful_query(indexing, *duration),
                        Err(error) => {
                            isa.observe_failed_query(indexing, *duration, error.is_timeout());
                            if let IndexerErrorObservation::IndexingBehind {
                                refreshed_requirements,
                                latest,
                            } = error
                            {
                                isa.observe_indexing_behind(
                                    indexing,
                                    refreshed_requirements.minimum_block.clone(),
                                    *latest,
                                )
                            }
                        }
                    },
                    Update::Penalty { indexing, weight } => isa.penalize(indexing, *weight),
                }
            }
        })
        .await;
    event_buffer.clear();
}
