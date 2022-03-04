use std::env;
use std::time::Duration;
use std::{thread, time};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::producer::Producer;

use super::client::KafkaClient;
use super::utils::{setup_logger, MessageKind};
use crate::redpanda::messages::{
    client_query_result::ClientQueryResult, indexer_attempt::IndexerAttempt,
    isa_scoring_error::ISAScoringError, isa_scoring_sample::ISAScoringSample,
};

use super::client::{CustomContext, LoggingConsumer};

async fn create_topic(
    brokers: String,
    ca: String,
    cert: String,
    key: String,
    user: String,
    pass: String,
    sec: String,
    mech: String,
    broker_ver: String,
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
    vec.push(("broker.version.fallback", broker_ver.as_str()));

    let kclient: KafkaClient = KafkaClient::new(
        &brokers[..],
        "rust-gateway-producer",
        vec.clone().as_slice(),
    )
    .expect("couldn't create client");

    kclient
        .create_topic("ephemeral_gateway_topic", 1, 1, vec, None)
        .await
        .unwrap();
}

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

    let kclient: KafkaClient = KafkaClient::new(
        &brokers[..],
        "rust-gateway-producer",
        vec.clone().as_slice(),
    )
    .expect("couldn't create client");

    let mut new_config = ClientConfig::new();
    new_config.set("bootstrap.servers", &brokers[..]);
    new_config.set("enable.partition.eof", "false");
    new_config.set("session.timeout.ms", "6000");
    new_config.set("enable.auto.commit", "false");
    new_config.set_log_level(RDKafkaLogLevel::Debug);

    for (key, val) in vec {
        new_config.set(key, val);
    }

    let test_isa_error = ISAScoringError {
        ray_id: "null-ray".to_string(),
        query_id: 123456,
        deployment: "Qmsdadasdghrfhefjhad".to_string(),
        indexer: "0xassd3rr4f4r1".to_string(),
        scoring_err: "Test Error".to_string(),
    };

    let isa_error_bytes = test_isa_error.write(MessageKind::JSON);

    kclient.send("test_gateway_topic", &isa_error_bytes);

    kclient.producer.flush(Duration::from_secs(1));
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
    //clone one object
    let futures = run_redpanda(
        brokers.clone(),
        ca.clone(),
        cert.clone(),
        key.clone(),
        user.clone(),
        pass.clone(),
        sec.clone(),
        mech.clone(),
    );
    futures.await;
}

#[tokio::test]
async fn test_topic_creation() {
    setup_logger(true, Some("rdkafka=trace"));

    let brokers = env::var("REDPANDA_BROKERS").unwrap();
    let ca = env::var("REDPANDA_SSL_CA").unwrap();
    let cert = env::var("REDPANDA_SSL_CERT").unwrap();
    let key = env::var("REDPANDA_SSL_KEY").unwrap();
    let user = env::var("REDPANDA_SASL_USER").unwrap();
    let pass = env::var("REDPANDA_SASL_PASSWORD").unwrap();
    let sec = env::var("REDPANDA_SECURITY_PROTOCOL").unwrap();
    let mech = env::var("REDPANDA_SASL_MECHANISM").unwrap();
    let broker_ver = env::var("BROKER_VERSION").unwrap();

    //clone one object
    let result = create_topic(
        brokers.clone(),
        ca.clone(),
        cert.clone(),
        key.clone(),
        user.clone(),
        pass.clone(),
        sec.clone(),
        mech.clone(),
        broker_ver.clone(),
    );
    result.await
}
