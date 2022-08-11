use super::{IndexerError, Indexing};

use {
    crate::{
        indexer_selection::Indexers,
        prelude::GRT,
        utils::{buffer_queue::QueueReader, double_buffer::DoubleBufferWriter},
    },
    tokio::{
        select,
        time::{sleep_until, Duration, Instant},
    },
};

pub struct QueryObservationErrorKind {
    is_timeout: bool,
}

pub enum Update {
    UsdToGRTConversion(GRT),
    QueryObservation {
        indexing: Indexing,
        duration: Duration,
        result: Result<(), QueryObservationErrorKind>,
        receipt: Vec<u8>,
    },
    Penalty {
        indexing: Indexing,
        weight: u8,
    },
}

pub async fn process_updates(
    mut writer: DoubleBufferWriter<Indexers>,
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
                process_events(&mut event_buffer, &mut writer).await;
            },
        }
    }
}

async fn process_events(event_buffer: &mut Vec<Update>, writer: &mut DoubleBufferWriter<Indexers>) {
    writer
        .update(|indexers| {
            for event in event_buffer.iter() {
                match event {
                    Update::UsdToGRTConversion(usd_to_grt) => {
                        indexers.network_params.usd_to_grt_conversion = Some(*usd_to_grt);
                    }
                    Update::QueryObservation {
                        indexing,
                        duration,
                        result,
                    } => match result {
                        Ok(()) => indexers.observe_successful_query(indexing, *duration, &receipt),
                        Err(error) => {
                            indexers.observe_failed_query(indexing, *duration, receipt, error)
                        }
                    },
                    Update::Penalty { indexing, weight } => indexers.penalize(indexing, *weight),
                }
            }
        })
        .await;
    event_buffer.clear();
}
