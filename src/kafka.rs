use std::time::Duration;

use anyhow::{Context as _, anyhow};
use rdkafka::{
    Message as _, Offset, TopicPartitionList,
    consumer::{Consumer as _, StreamConsumer},
    message::OwnedMessage,
};

const TIMEOUT: Duration = Duration::from_secs(30);

pub fn assign_partitions(
    consumer: &StreamConsumer,
    topic: &str,
    offset: Offset,
) -> anyhow::Result<()> {
    let mut assignment = TopicPartitionList::new();
    for partition_id in fetch_partition_ids(consumer.client(), topic)? {
        assignment.add_partition_offset(topic, partition_id, offset)?;
    }
    tracing::debug!(?assignment);
    consumer.assign(&assignment)?;
    Ok(())
}

pub async fn latest_timestamp(
    consumer: &StreamConsumer,
    topic: &str,
) -> anyhow::Result<Option<i64>> {
    let mut latest_messages = fetch_messages(consumer, topic, Offset::OffsetTail(1)).await?;
    assert!(latest_messages.len() <= 1, "multiple topic partitions");
    let message = match latest_messages.pop() {
        Some(message) => message,
        None => return Ok(None),
    };
    Ok(message.timestamp().to_millis())
}

async fn fetch_messages(
    consumer: &StreamConsumer,
    topic: &str,
    offset: Offset,
) -> anyhow::Result<Vec<OwnedMessage>> {
    let mut result: Vec<OwnedMessage> = Default::default();
    let mut assignment = TopicPartitionList::new();
    for partition in fetch_partition_ids(consumer.client(), topic)? {
        // skip empty partitions
        let watermarks = consumer.fetch_watermarks(topic, partition, TIMEOUT)?;
        if (watermarks.1 - watermarks.0) > 0 {
            assignment.add_partition_offset(topic, partition, offset)?;
        }
    }
    if assignment.elements().is_empty() {
        return Ok(result);
    }
    consumer
        .assign(&assignment)
        .with_context(|| anyhow!("assign {topic} partitions"))?;
    for _ in 0..assignment.elements().len() {
        let msg = consumer.recv().await.context("fetch latest message")?;
        result.push(msg.detach());
    }
    Ok(result)
}

fn fetch_partition_ids<C>(
    client: &rdkafka::client::Client<C>,
    topic: &str,
) -> anyhow::Result<Vec<i32>>
where
    C: rdkafka::client::ClientContext,
{
    let metadata = client
        .fetch_metadata(Some(topic), TIMEOUT)
        .with_context(|| anyhow!("fetch {topic} metadata"))?;
    anyhow::ensure!(!metadata.topics().is_empty());
    let topic_info = &metadata.topics()[0];
    Ok(topic_info.partitions().iter().map(|p| p.id()).collect())
}
