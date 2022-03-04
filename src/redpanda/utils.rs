use anyhow::bail;
use chrono::prelude::*;
use env_logger::{fmt::Formatter, Builder};
use log::{LevelFilter, Record};
use std::{fmt::Display, io::Write, thread, time::Duration};

use rdkafka::{
    client::Client,
    consumer::ConsumerContext,
    producer::{DefaultProducerContext, DeliveryResult, ProducerContext},
    ClientContext,
};

use tracing::{debug, error, info, warn};

/// Different serialization formats. @TODO: Overrwrite w/ schema registry.
pub enum MessageKind {
    JSON,
    AVRO,
    OTHER,
}

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |formatter: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        write!(
            formatter,
            "{} {}{} - {} - {}\n",
            time_str,
            thread_name,
            record.level(),
            record.target(),
            record.args()
        )
    };

    let mut builder = Builder::new();
    builder
        .format(output_format)
        .filter(None, LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}

pub trait CollectionExt<T>: Sized
where
    T: IntoIterator,
{
    /// Consumes the collection and returns its first element.
    ///
    /// This method panics if the collection does not have at least one element.
    fn into_first(self) -> T::Item;

    /// Consumes the collection and returns its last element.
    ///
    /// This method panics if the collection does not have at least one element.
    fn into_last(self) -> T::Item;

    /// Consumes the collection and returns its only element.
    ///
    /// This method panics if the collection does not have exactly one element.
    fn into_element(self) -> T::Item {
        self.expect_element("into_element called on collection with more than one element")
    }

    /// Consumes the collection and returns its only element.
    ///
    /// This method panics with the given error message if the collection does not have exactly one element.
    fn expect_element<Err: Display>(self, msg: Err) -> T::Item;
}

impl<T> CollectionExt<T> for T
where
    T: IntoIterator,
{
    fn into_first(self) -> T::Item {
        self.into_iter().next().unwrap()
    }

    fn into_last(self) -> T::Item {
        self.into_iter().last().unwrap()
    }

    fn expect_element<Err: Display>(self, msg: Err) -> T::Item {
        let mut iter = self.into_iter();
        match (iter.next(), iter.next()) {
            (Some(el), None) => el,
            _ => panic!("{}", msg),
        }
    }
}

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
