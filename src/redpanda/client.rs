use rdkafka::types::RDKafkaErrorCode;
use std::{fmt, thread, time::Duration};

use rdkafka::{
    client::ClientContext,
    config::ClientConfig,
    consumer::{ConsumerContext, Rebalance},
    error::{KafkaError, KafkaResult},
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    topic_partition_list::TopicPartitionList,
};

use log::info;

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
        config.set("message.timeout.ms", "3000");
        config.set("queue.buffering.max.ms", "1000");
        config.set("queue.buffering.max.messages", "1000000");
        config.set("allow.auto.create.topics", "true");

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

    pub fn send(&self, topic_name: &str, message: &[u8]) {
        let mut record = BaseRecord::<'_, (), [u8]>::to(topic_name).payload(message);

        loop {
            match self.producer.send(record) {
                Ok(()) => break,
                //if the queue is full, try again.
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rec)) => {
                    // Retry after 500ms
                    record = rec;
                    thread::sleep(Duration::from_millis(500));
                }

                //otherwise just break
                Err((e, _)) => {
                    println!("Failed to publish on kafka {:?}", e);
                    break;
                }
            }
        }
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
