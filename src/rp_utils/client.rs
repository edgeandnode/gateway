#![allow(dead_code)]
#![allow(unused_variables)]
use super::collections::CollectionExt;
use anyhow::bail;
use rdkafka::client::Client;
use rdkafka::consumer::ConsumerContext;
use rdkafka::producer::{DefaultProducerContext, DeliveryResult, ProducerContext};
use rdkafka::ClientContext;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub struct RpClientContext;

impl ClientContext for RpClientContext {
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel::*;
        // Copied from https://docs.rs/rdkafka/0.28.0/src/rdkafka/client.rs.html#58-79
        // but using `tracing`
        match level {
            Emerg | Alert | Critical | Error => {
                error!(target: "librdkafka", "{} {}", fac, log_message);
            }
            Warning => warn!(target: "librdkafka", "{} {}", fac, log_message),
            Notice => info!(target: "librdkafka", "{} {}", fac, log_message),
            Info => info!(target: "librdkafka", "{} {}", fac, log_message),
            Debug => debug!(target: "librdkafka", "{} {}", fac, log_message),
        }
    }
    // Refer to the comment on the `log` callback.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
    }
}

impl ConsumerContext for RpClientContext {}
impl ProducerContext for RpClientContext {
    type DeliveryOpaque = <DefaultProducerContext as ProducerContext>::DeliveryOpaque;
    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        DefaultProducerContext.delivery(delivery_result, delivery_opaque);
    }
}

pub fn get_partitions<C: ClientContext>(
    client: &Client<C>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, anyhow::Error> {
    let meta = client.fetch_metadata(Some(&topic), timeout)?;
    if meta.topics().len() != 1 {
        bail!(
            "topic {} has {} metadata entries; expected 1",
            topic,
            meta.topics().len()
        );
    }
    let meta_topic = meta.topics().into_element();
    if meta_topic.name() != topic {
        bail!(
            "got results for wrong topic {} (expected {})",
            meta_topic.name(),
            topic
        );
    }

    if meta_topic.partitions().len() == 0 {
        bail!("topic {} does not exist", topic);
    }

    Ok(meta_topic.partitions().iter().map(|x| x.id()).collect())
}
