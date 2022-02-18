use std::time::Duration;

use anyhow::Context;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::producer::{
    BaseRecord, DefaultProducerContext, DeliveryFuture, FutureProducer, FutureRecord,
    ThreadedProducer,
};
use rdkafka::topic_partition_list::TopicPartitionList;

use log::info;
use std::fmt;

use super::utils::{setup_logger, MessageKind};
use crate::rp_utils::client::RpClientContext;

// #[derive(Clone)]
pub struct KafkaClient {
    pub(super) producer: ThreadedProducer<DefaultProducerContext>,
    // pub(super) thread_producer: FutureProducer<RpClientContext>,
    kafka_url: String,
}

impl fmt::Debug for KafkaClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Brokers running at: {}", self.kafka_url)
    }
}

impl KafkaClient {
    pub fn new(
        kafka_url: &str,
        group_id: &str,
        configs: &[(&str, &str)],
    ) -> Result<KafkaClient, anyhow::Error> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_url);
        config.set("group.id", group_id);

        for (key, val) in configs {
            config.set(*key, *val);
        }

        let producer = config
            .create_with_context(DefaultProducerContext)
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

    pub async fn delete_topic(
        &self,
        topic_name: &str,
        configs: Vec<(&str, &str)>,
        timeout: Option<Duration>,
    ) -> Result<(), anyhow::Error> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.kafka_url);

        let client = config
            .create::<AdminClient<_>>()
            .expect("creating admin kafka client failed");

        let admin_opts = AdminOptions::new().request_timeout(timeout);

        let mut topics = [""; 1];
        topics.fill(topic_name);
        client
            .delete_topics(&topics, &admin_opts)
            .await
            .context(format!("deleting Kafka topic: {}", topic_name))?;

        Ok(())
    }

    pub fn send(&self, topic_name: &str, message: &[u8]) {
        //     let record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&topic_name)
        //         .payload(message)
        //         .timestamp(chrono::Utc::now().timestamp_millis());
        //     self.producer.send_result(record).map_err(|(e, _message)| e)

        let mut record = BaseRecord::to(topic_name).payload(message).key("1");
        match self.producer.send(record) {
            Ok(()) => (),
            _ => (),
        }
    }

    // pub fn send_key_value(
    //     &self,
    //     topic_name: &str,
    //     key: &[u8],
    //     message: Option<Vec<u8>>,
    // ) -> Result<DeliveryFuture, KafkaError> {
    //     let mut record: FutureRecord<_, _> = FutureRecord::to(&topic_name)
    //         .key(key)
    //         .timestamp(chrono::Utc::now().timestamp_millis());
    //     if let Some(message) = &message {
    //         record = record.payload(message);
    //     }
    //     self.producer.send_result(record).map_err(|(e, _message)| e)
    // }
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
