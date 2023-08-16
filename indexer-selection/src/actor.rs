use std::collections::HashMap;

use tokio::{
    select,
    time::{sleep_until, Duration, Instant},
};

use prelude::*;
use prelude::{
    buffer_queue::{Event, QueueReader},
    double_buffer::DoubleBufferWriter,
};

use crate::{IndexerErrorObservation, Indexing, IndexingStatus, State};

#[derive(Debug)]
pub enum Update {
    USDToGRTConversion(GRT),
    SlashingPercentage(PPM),
    Indexings(HashMap<Indexing, IndexingStatus>),
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
                writer
                    .update(|state| {
                        for update in event_buffer.iter() {
                            if let Event::Update(update) = update {
                                apply_state_update(state, update);
                            }
                        }
                    })
                    .await;
                for update in event_buffer.iter() {
                    if let Event::Flush(notify) = update {
                        notify.notify_one();
                    }
                }
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
        Update::Indexings(indexings) => {
            for (indexing, update) in indexings {
                state.insert_indexing(*indexing, update.clone());
            }
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
