use chrono::{DateTime, NaiveDateTime, Utc};
use prelude::{anyhow::anyhow, *};
use serde::{de::Error, Deserialize, Deserializer};
use std::collections::HashMap;

#[derive(Clone)]
pub struct Subscriptions {
    pub active_subscriptions: Eventual<Ptr<HashMap<Address, ActiveSubscription>>>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: Address,
    pub authorized_signers: Option<Vec<AuthorizedSigner>>,
}

#[derive(Clone, Deserialize)]
pub struct ActiveSubscription {
    pub id: Bytes32,
    pub user: User,
    #[serde(deserialize_with = "deserialize_datetime_utc")]
    pub start: DateTime<Utc>,
    #[serde(deserialize_with = "deserialize_datetime_utc")]
    pub end: DateTime<Utc>,
    pub rate: u128,
}
impl TryFrom<(Bytes32, User, u64, u64, u128)> for ActiveSubscription {
    type Error = anyhow::Error;
    fn try_from(from: (Bytes32, User, u64, u64, u128)) -> Result<Self, Self::Error> {
        let (id, user, start, end, rate) = from;
        let to_datetime = |t: u64| {
            NaiveDateTime::from_timestamp_opt(t.try_into()?, 0)
                .ok_or_else(|| anyhow!("invalid timestamp"))
                .map(|t| DateTime::<Utc>::from_utc(t, Utc))
        };
        let start = to_datetime(start)?;
        let end = to_datetime(end)?;
        Ok(Self {
            id,
            user,
            start,
            end,
            rate,
        })
    }
}
fn deserialize_datetime_utc<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let input = String::deserialize(deserializer)?;
    let timestamp = u64::from_str_radix(input.split_at(2).1, 16).map_err(D::Error::custom)?;
    NaiveDateTime::from_timestamp_opt(timestamp.try_into().map_err(D::Error::custom)?, 0)
        .ok_or_else(|| D::Error::custom("invalid timestamp"))
        .map(|t| DateTime::<Utc>::from_utc(t, Utc))
}

#[derive(Clone, Deserialize)]
pub struct AuthorizedSigner {
    pub id: Bytes32,
    pub user: User,
    pub signer: Address,
}

impl Subscriptions {
    pub async fn active_subscription(&self, user: &Address) -> Option<ActiveSubscription> {
        self.active_subscriptions
            .value()
            .await
            .ok()?
            .get(user)
            .cloned()
    }

    pub async fn has_active_subscription(&self, user: &Address, timestamp: DateTime<Utc>) -> bool {
        match self.active_subscription(user).await {
            Some(subscription) => subscription.start <= timestamp && subscription.end >= timestamp,
            None => false,
        }
    }
}
