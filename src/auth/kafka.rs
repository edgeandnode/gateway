use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use prost::Message as _;
use rdkafka::{Message as _, Offset, consumer::StreamConsumer};
use tokio::sync::watch;
use url::Url;

use crate::{
    KafkaConsumer,
    auth::ApiKey,
    kafka::{assign_partitions, latest_timestamp},
};

pub async fn api_keys(
    consumer: &KafkaConsumer,
    topic: &str,
    boostrap_url: &Url,
    bootstrap_auth: &str,
) -> anyhow::Result<watch::Receiver<HashMap<String, ApiKey>>> {
    let recent_timestamp = {
        let consumer = consumer.stream_consumer(topic)?;
        let recent_timestamp = latest_timestamp(&consumer, topic).await?;
        assign_partitions(&consumer, topic, Offset::Beginning)?;
        recent_timestamp
    }; // drop stream consumer to avoid issues with cached BEGINNING offset
    tracing::debug!(?recent_timestamp);
    let consumer = consumer.stream_consumer(topic)?;

    #[derive(Default)]
    struct Status {
        latest_timestamp: Option<i64>,
        latest_bootstrap: Option<i64>,
    }
    let (tx, rx) = watch::channel::<HashMap<String, ApiKey>>(Default::default());
    let (status_tx, mut status_rx) = watch::channel::<Status>(Default::default());
    tokio::spawn(async move {
        let mut api_keys: HashMap<i32, String> = Default::default();
        loop {
            let event = match recv_event(&consumer).await {
                Ok(event) => event,
                Err(api_key_event_err) => {
                    tracing::error!(%api_key_event_err);
                    continue;
                }
            };
            let (timestamp, boostrap) =
                (event.timestamp, event.event_type() == EventType::Bootstrap);

            handle_event(&mut api_keys, &tx, event);

            status_tx.send_modify(|status| {
                status.latest_timestamp = Some(
                    status
                        .latest_timestamp
                        .map(|t| i64::max(t, timestamp))
                        .unwrap_or(timestamp),
                );
                if boostrap {
                    status.latest_bootstrap = status.latest_timestamp;
                }
            });
        }
    });

    let latest_bootstrap: i64 = match recent_timestamp {
        None => 0,
        Some(recent_timestamp) => {
            status_rx
                .wait_for(|status| status.latest_timestamp == Some(recent_timestamp))
                .await
                .unwrap();
            status_rx.borrow().latest_bootstrap.unwrap_or(0)
        }
    };

    let seconds_since_boostrap = (SystemTime::UNIX_EPOCH
        + Duration::from_millis(latest_bootstrap as u64))
    .elapsed()
    .unwrap();
    if seconds_since_boostrap < Duration::from_secs(30) {
        tracing::info!("bootstrap event received, waiting 30s");
        tokio::time::sleep(Duration::from_secs(30)).await;
    } else if seconds_since_boostrap > Duration::from_secs(86_400) {
        tracing::info!("requesting bootstrap events");
        let client = reqwest::Client::new();
        client
            .post(boostrap_url.clone())
            .bearer_auth(bootstrap_auth)
            .send()
            .await?
            .error_for_status()?;
        status_rx
            .wait_for(|status| {
                status
                    .latest_bootstrap
                    .map(|t| t >= latest_bootstrap)
                    .unwrap_or(false)
            })
            .await
            .unwrap();
        tracing::debug!("bootstrap event received, waiting 30s");
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    tracing::info!(api_keys = rx.borrow().len());

    Ok(rx)
}

async fn recv_event(consumer: &StreamConsumer) -> anyhow::Result<ApiKeyEvent> {
    let msg = consumer.recv().await?;
    let payload = msg.payload().ok_or_else(|| anyhow!("missing payload"))?;
    let mut event = ApiKeyEvent::decode(payload)?;
    if let Some(timestamp) = msg.timestamp().to_millis() {
        event.timestamp = timestamp;
    }
    Ok(event)
}

fn handle_event(
    keys: &mut HashMap<i32, String>,
    tx: &watch::Sender<HashMap<String, ApiKey>>,
    event: ApiKeyEvent,
) {
    match event.event_type() {
        EventType::ApiKeyDeleted => {
            if let Some(removed) = keys.remove(&event.id) {
                tx.send_modify(|map| {
                    map.remove(&removed);
                });
            }
            return;
        }
        EventType::Bootstrap
        | EventType::ApiKeyCreated
        | EventType::ApiKeyRotated
        | EventType::BillingBalanceChange
        | EventType::PaymentMethodSwitch
        | EventType::InvoicePaid
        | EventType::InvoicePaymentFailed
        | EventType::ApiKeyAuthorizedDomainAdded
        | EventType::ApiKeyAuthorizedDomainRemoved
        | EventType::ApiKeyAuthorizedSubgraphAdded
        | EventType::ApiKeyAuthorizedSubgraphRemoved
        | EventType::ApiKeyMonthlyCapChanged
        | EventType::QueriesReceived => (),
    };
    let key = ApiKey {
        query_status: match event.query_status() {
            QueryStatus::Active => crate::auth::QueryStatus::Active,
            QueryStatus::ServiceShutoff => crate::auth::QueryStatus::ServiceShutoff,
            QueryStatus::MonthlyCapReached => crate::auth::QueryStatus::MonthlyCapReached,
        },
        key: event.key,
        user_address: event.user_address,
        subgraphs: event
            .subgraphs
            .into_iter()
            .filter_map(|s| s.parse().ok())
            .collect(),
        domains: event.domains,
    };
    keys.insert(event.id, key.key.clone());
    tx.send_modify(move |map| {
        map.insert(key.key.clone(), key);
    });
}

#[derive(prost::Message)]
struct ApiKeyEvent {
    /// timestamp of when the api key event occurred
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    /// ID of the api key
    #[prost(int32, tag = "2")]
    pub id: i32,
    /// Unique key. Used by users to query a Subgraph through the gateway, as an authorization bearer token.
    #[prost(string, tag = "3")]
    pub key: String,
    /// ID of the User that owns the api key
    #[prost(int32, tag = "4")]
    pub user_id: i32,
    /// User 0x ETH address that owns the api key
    #[prost(string, tag = "5")]
    pub user_address: String,
    /// The derived query status of the api key
    #[prost(enumeration = "QueryStatus", tag = "6")]
    pub query_status: i32,
    /// List of domains that can query Subgraphs using this api key.
    /// If empty, all domains are allowed
    #[prost(string, repeated, tag = "7")]
    pub domains: Vec<String>,
    /// List of Subgraphs that the api key can query.
    /// If empty, all Subgraphs are allowed
    #[prost(string, repeated, tag = "8")]
    pub subgraphs: Vec<String>,
    /// The event that caused the change/initiated sending this message
    #[prost(enumeration = "EventType", tag = "9")]
    pub event_type: i32,
}

/// The derived query status of the api key
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum QueryStatus {
    /// the api key is active. can be used to query subgraphs.
    #[prost(Name = "ACTIVE", tag = "0")]
    Active = 0,
    /// the api key is shutoff due to query fees exceeding billing balance.
    #[prost(Name = "SERVICE_SHUTOFF", tag = "1")]
    ServiceShutoff = 1,
    /// the api key is shutoff due to the query fees exceeding the monthly cap assigned to the api key.
    #[prost(Name = "MONTHLY_CAP_REACHED", tag = "2")]
    MonthlyCapReached = 2,
}

/// The event that caused the change/initiated sending this message
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum EventType {
    /// The api key was created
    #[prost(Name = "API_KEY_CREATED", tag = "0")]
    ApiKeyCreated = 0,
    /// The key on the api key was rotated
    #[prost(Name = "API_KEY_ROTATED", tag = "1")]
    ApiKeyRotated = 1,
    /// The api key was deleted
    #[prost(Name = "API_KEY_DELETED", tag = "2")]
    ApiKeyDeleted = 2,
    /// The user billing balance changed
    #[prost(Name = "BILLING_BALANCE_CHANGE", tag = "3")]
    BillingBalanceChange = 3,
    /// The user switched payment methods (crypto -> CC, CC -> crypto)
    #[prost(Name = "PAYMENT_METHOD_SWITCH", tag = "4")]
    PaymentMethodSwitch = 4,
    /// The user paid off an invoice
    #[prost(Name = "INVOICE_PAID", tag = "5")]
    InvoicePaid = 5,
    /// The Customer failed to pay their Invoice.
    #[prost(Name = "INVOICE_PAYMENT_FAILED", tag = "6")]
    InvoicePaymentFailed = 6,
    /// The user added an authorized domain for the api key
    #[prost(Name = "API_KEY_AUTHORIZED_DOMAIN_ADDED", tag = "7")]
    ApiKeyAuthorizedDomainAdded = 7,
    /// The user removed an authorized domain from the api key
    #[prost(Name = "API_KEY_AUTHORIZED_DOMAIN_REMOVED", tag = "8")]
    ApiKeyAuthorizedDomainRemoved = 8,
    /// The user added an authorized subgraph for the api key
    #[prost(Name = "API_KEY_AUTHORIZED_SUBGRAPH_ADDED", tag = "9")]
    ApiKeyAuthorizedSubgraphAdded = 9,
    /// The user removed an authorized subgraph from the api key
    #[prost(Name = "API_KEY_AUTHORIZED_SUBGRAPH_REMOVED", tag = "10")]
    ApiKeyAuthorizedSubgraphRemoved = 10,
    /// The user updated the monthly cap on the api key.
    #[prost(Name = "API_KEY_MONTHLY_CAP_CHANGED", tag = "11")]
    ApiKeyMonthlyCapChanged = 11,
    /// The user queried the api key, and the query counts/fees affected the query status
    #[prost(Name = "QUERIES_RECEIVED", tag = "12")]
    QueriesReceived = 12,
    /// Bootstrap event from the gateway to initialize its cache of api keys
    #[prost(Name = "BOOTSTRAP", tag = "13")]
    Bootstrap = 13,
}
