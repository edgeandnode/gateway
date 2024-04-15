use std::fmt;

use rdkafka::error::KafkaResult;
use serde_json::Map;
use tracing::span;
use tracing_subscriber::{layer, Layer};

use super::logging::error_log;

pub struct KafkaClient {
    producer: rdkafka::producer::ThreadedProducer<rdkafka::producer::DefaultProducerContext>,
}

impl KafkaClient {
    pub fn new(config: &rdkafka::ClientConfig) -> KafkaResult<KafkaClient> {
        let producer = config.create_with_context(rdkafka::producer::DefaultProducerContext)?;
        Ok(KafkaClient { producer })
    }

    pub fn send(&self, topic: &str, payload: &[u8]) {
        // Don't bother attempting to send messages that the broker should reject.
        const MAX_MSG_BYTES: usize = 1 << 20;
        if payload.len() > MAX_MSG_BYTES {
            tracing::warn!(kafka_producer_err = "msg too big");
        }

        let record = rdkafka::producer::BaseRecord::<'_, (), [u8]>::to(topic).payload(payload);
        if let Err((kafka_producer_err, _)) = self.producer.send(record) {
            tracing::error!(%kafka_producer_err, %topic);
        }
    }
}

pub struct EventHandlerFn<
    F = fn(&KafkaClient, &tracing::Metadata<'_>, Map<String, serde_json::Value>),
>(F);

impl<F> EventHandlerFn<F>
where
    F: Fn(&KafkaClient, &tracing::Metadata<'_>, Map<String, serde_json::Value>),
{
    pub fn new(f: F) -> Self {
        EventHandlerFn(f)
    }

    pub fn call(
        &self,
        client: &KafkaClient,
        metadata: &tracing::Metadata<'_>,
        fields: Map<String, serde_json::Value>,
    ) {
        (self.0)(client, metadata, fields)
    }
}

pub struct KafkaLayer {
    pub client: &'static KafkaClient,
    pub event_handler: EventHandlerFn,
}

impl<S> Layer<S> for KafkaLayer
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        let mut fields: Map<String, serde_json::Value> = Map::new();
        // insert values from parent spans
        for span in ctx.span_scope(id).unwrap().skip(1) {
            let extensions = span.extensions();
            if let Some(scope_fields) = extensions.get::<Map<String, serde_json::Value>>() {
                for (key, value) in scope_fields {
                    // favor values set in inner scopes
                    if !fields.contains_key(key) {
                        fields.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        attrs.record(&mut CollectFields(&mut fields));
        let span = ctx.span(id).unwrap();
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: layer::Context<'_, S>) {
        let span = ctx.span(span).unwrap();
        let mut extensions = span.extensions_mut();
        let fields = match extensions.get_mut() {
            Some(fields) => fields,
            None => {
                error_log(span.metadata().target(), "report already sent for span");
                return;
            }
        };
        values.record(&mut CollectFields(fields));
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: layer::Context<'_, S>) {
        let span = ctx.event_span(event).unwrap();
        let mut extensions = span.extensions_mut();
        let fields = match extensions.get_mut() {
            Some(fields) => fields,
            None => {
                error_log(span.metadata().target(), "report already sent for span");
                return;
            }
        };
        event.record(&mut CollectFields(fields));

        // TODO: ideally we would always send the message when span closes, but it seems we can't
        // access the stored fields in `on_close`.
        if !fields.contains_key("status_message") {
            return;
        }

        let fields: Map<String, serde_json::Value> = extensions.remove().unwrap();
        self.event_handler
            .call(self.client, event.metadata(), fields);
    }
}

struct CollectFields<'a>(&'a mut Map<String, serde_json::Value>);
impl tracing_subscriber::field::Visit for CollectFields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), format!("{:?}", value).into());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.0
            .insert(field.name().to_owned(), value.to_string().into());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        debug_assert!(false, "u128 should not be serialized to JSON");
        self.0
            .insert(field.name().to_owned(), value.to_string().into());
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        debug_assert!(false, "i128 should not be serialized to JSON");
        self.0
            .insert(field.name().to_owned(), value.to_string().into());
    }
}
