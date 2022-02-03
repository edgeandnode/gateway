use std::env;
use std::thread;
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
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Message;

use log::info;

use super::client::KafkaClient;
use super::messages::indexer_score;
use super::test_utils::*;
use super::utils::{setup_logger, MessageKind};
use crate::rp_utils::client::RpClientContext;

use super::client::{CustomContext, LoggingConsumer};

async fn run_redpanda(
    brokers: String,
    ca: String,
    cert: String,
    key: String,
    user: String,
    pass: String,
    sec: String,
    mech: String,
) {
    println!("{}", brokers);
    println!("{}", ca);
    println!("{}", cert);
    println!("{}", key);
    println!("{}", user);
    println!("{}", pass);
    println!("{}", sec);
    println!("{}", mech);

    let mut vec = vec![];
    vec.push(("bootstrap.servers", brokers.as_str()));
    vec.push(("sasl.password", pass.as_str()));
    vec.push(("sasl.username", user.as_str()));
    vec.push(("sasl.mechanism", mech.as_str()));
    vec.push(("ssl.ca.location", ca.as_str()));
    vec.push(("ssl.certificate.location", cert.as_str()));
    vec.push(("ssl.key.location", key.as_str()));
    vec.push(("security.protocol", sec.as_str()));
    vec.push(("debug", "all"));

    let kclient: KafkaClient = KafkaClient::new(&brokers[..], "rust-gateway-producer", vec.clone())
        .expect("couldn't create client");

    let mut new_config = ClientConfig::new();
    new_config.set("group.id", "rust-gateway-consumer");
    new_config.set("bootstrap.servers", &brokers[..]);
    new_config.set("enable.partition.eof", "false");
    new_config.set("session.timeout.ms", "6000");
    new_config.set("enable.auto.commit", "false");
    new_config.set_log_level(RDKafkaLogLevel::Debug);

    for (key, val) in vec {
        new_config.set(key, val);
    }

    let context = CustomContext;

    let consumer: LoggingConsumer = new_config
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["test123"])
        .expect("Can't subscribe to specified topic");

    let producer = kclient.producer;

    let test_payload = indexer_score::IndexerScoreMessage::default();

    let payload_bytes = test_payload.write(MessageKind::AVRO);

    let produce_future = producer.send(
        FutureRecord::to("test_gateway_topic")
            .key("some key")
            .payload(&payload_bytes),
        Duration::from_secs(0),
    );

    produce_future.await;

    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();

        async move {
            let owned_message = borrowed_message.detach();
            record_owned_message_receipt(&owned_message).await;

            tokio::spawn(async move {
                let computation_result =
                    tokio::task::spawn_blocking(|| expensive_computation(owned_message))
                        .await
                        .expect("failed to wait for expensive computation");
                let produce_future = producer.send(
                    FutureRecord::to("test_gateway_topic")
                        .key("some key")
                        .payload(&computation_result),
                    Duration::from_secs(0),
                );
                match produce_future.await {
                    Ok(delivery) => println!("Sent: {:?}", delivery),
                    Err((e, _)) => println!("Error: {:?}", e),
                }
            });
            Ok(())
        }
    });

    println!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    println!("Stream processing terminated");
}

#[tokio::test]
async fn test_redpanda() {
    setup_logger(true, Some("rdkafka=trace"));

    let brokers = env::var("REDPANDA_BROKERS").unwrap();
    let ca = env::var("REDPANDA_SSL_CA").unwrap();
    let cert = env::var("REDPANDA_SSL_CERT").unwrap();
    let key = env::var("REDPANDA_SSL_KEY").unwrap();
    let user = env::var("REDPANDA_SASL_USER").unwrap();
    let pass = env::var("REDPANDA_SASL_PASSWORD").unwrap();
    let sec = env::var("REDPANDA_SECURITY_PROTOCOL").unwrap();
    let mech = env::var("REDPANDA_SASL_MECHANISM").unwrap();

    let futures = (0..1).map(|_| {
        tokio::spawn(run_redpanda(
            brokers.clone(),
            ca.clone(),
            cert.clone(),
            key.clone(),
            user.clone(),
            pass.clone(),
            sec.clone(),
            mech.clone(),
        ))
    });
    let collected = futures.collect::<FuturesUnordered<_>>();
    let as_awaited = collected.for_each(|result| async { result.unwrap() });
    as_awaited.await;
}
