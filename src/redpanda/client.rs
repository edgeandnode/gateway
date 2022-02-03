use std::env;
use std::time::Duration;

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::Message;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;

use log::info;

use super::utils::{setup_logger, MessageKind};
use crate::rp_utils::client::RpClientContext;

#[derive(Clone)]
pub struct KafkaClient {
    pub(super) producer: FutureProducer<RpClientContext>,
    kafka_url: String,
}

impl KafkaClient {
    pub fn new(
        kafka_url: &str,
        group_id: &str,
        configs: Vec<(&str, &str)>,
    ) -> Result<KafkaClient, anyhow::Error> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_url);
        config.set("group.id", group_id);

        for (key, val) in configs {
            config.set(key, val);
        }

        let producer = config
            .create_with_context(RpClientContext)
            .expect("Producer creation error");

        Ok(KafkaClient {
            producer,
            kafka_url: kafka_url.to_string(),
        })
    }

    pub async fn create_topic(
        &self,
        topic_name: &str,
        partitions: i32,
        replication: i32,
        configs: Vec<(&str, &str)>,
        timeout: Option<Duration>,
    ) -> Result<(), anyhow::Error> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.kafka_url);

        let client = config
            .create::<AdminClient<_>>()
            .expect("creating admin kafka client failed");

        let admin_opts = AdminOptions::new().request_timeout(timeout);

        let mut topic = NewTopic::new(topic_name, partitions, TopicReplication::Fixed(replication));
        for (key, val) in configs {
            topic = topic.set(key, val);
        }

        crate::rp_utils::admin::ensure_topic(&client, &admin_opts, &topic)
            .await
            .context(format!("creating Kafka topic: {}", topic_name))?;

        Ok(())
    }

    pub fn send(&self, topic_name: &str, message: &[u8]) -> Result<DeliveryFuture, KafkaError> {
        let record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&topic_name)
            .payload(message)
            .timestamp(chrono::Utc::now().timestamp_millis());
        self.producer.send_result(record).map_err(|(e, _message)| e)
    }

    pub fn send_key_value(
        &self,
        topic_name: &str,
        key: &[u8],
        message: Option<Vec<u8>>,
    ) -> Result<DeliveryFuture, KafkaError> {
        let mut record: FutureRecord<_, _> = FutureRecord::to(&topic_name)
            .key(key)
            .timestamp(chrono::Utc::now().timestamp_millis());
        if let Some(message) = &message {
            record = record.payload(message);
        }
        self.producer.send_result(record).map_err(|(e, _message)| e)
    }
}

pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// Type alias for custom consumer.
pub type LoggingConsumer = StreamConsumer<CustomContext>;
